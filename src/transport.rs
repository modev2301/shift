//! Transport abstraction: TCP implements today, QUIC implements same traits later.
//! Nothing above this layer changes when swapping transport.

use crate::base::TransferConfig;
use crate::error::TransferError;
use async_trait::async_trait;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

/// Platform for capability reporting (0=unknown, 1=linux, 2=windows).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Platform {
    Unknown = 0,
    Linux = 1,
    Windows = 2,
}

impl Default for Platform {
    fn default() -> Self {
        Self::Unknown
    }
}

/// A bidirectional stream with shutdown. Implementors must also implement AsyncRead + AsyncWrite + Unpin.
#[async_trait]
pub trait Stream: Send + Sync + AsyncRead + AsyncWrite + Unpin {
    async fn shutdown(&self) -> Result<(), TransferError>;
}

/// A connection that can open (client) or accept (server) streams.
#[async_trait]
pub trait Connection: Send + Sync {
    /// Client: open a new stream on this connection.
    async fn open_stream(&self) -> Result<Box<dyn Stream>, TransferError>;
    /// Server: accept a stream on this connection.
    async fn accept_stream(&self) -> Result<Box<dyn Stream>, TransferError>;
    fn platform(&self) -> Platform;
}

/// A listener that accepts connections.
#[async_trait]
pub trait Listener: Send + Sync {
    async fn accept(&self) -> Result<Box<dyn Connection>, TransferError>;
}

/// Metadata channel halves for the transfer protocol (capability handshake, metadata, 0x06/0x07, hash).
pub struct TransferMetaChannels {
    pub reader: Box<dyn tokio::io::AsyncRead + Send + Unpin>,
    pub writer: Box<dyn tokio::io::AsyncWrite + Send + Unpin>,
}

/// Opens data streams on demand (TCP: connect to next port, QUIC: open_bi). Coordinator holds this for transfer duration.
#[async_trait]
pub trait StreamOpener: Send + Sync {
    /// Open one new stream. TCP: connect to next data port; QUIC: open_bi on the connection.
    async fn open_stream(&self) -> Result<Box<dyn Stream>, TransferError>;
    fn max_streams(&self) -> usize;
}

/// Transport: connect (client) or listen (server). TCP and QUIC implement this.
#[async_trait]
pub trait Transport: Send + Sync {
    /// Display name for UI (e.g. "tcp", "quic").
    fn name(&self) -> &'static str {
        "tcp"
    }

    async fn connect(&self, addr: SocketAddr) -> Result<Box<dyn Connection>, TransferError>;
    async fn listen(&self, addr: SocketAddr) -> Result<Box<dyn Listener>, TransferError>;

    /// Establish metadata channel and a stream opener. Caller uses opener.open_stream() for data (initial + scale-up).
    /// num_streams: hint for initial count; max_streams: ceiling for opener. Opener is Arc so workers can share it.
    async fn connect_for_transfer(
        &self,
        addr: SocketAddr,
        num_streams: usize,
        max_streams: usize,
    ) -> Result<(TransferMetaChannels, Arc<dyn StreamOpener>), TransferError>;

    /// Deprecated: use StreamOpener from connect_for_transfer instead. Kept for compatibility.
    async fn open_data_connections(
        &self,
        _addr: SocketAddr,
        _num: usize,
    ) -> Result<Vec<Box<dyn Stream>>, TransferError> {
        Err(TransferError::ProtocolError(
            "open_data_connections not supported; use StreamOpener from connect_for_transfer".to_string(),
        ))
    }
}

// -----------------------------------------------------------------------------
// TCP implementation
// -----------------------------------------------------------------------------

use socket2::{Domain, Socket, Type};
use tokio::net::{TcpListener as TokioTcpListener, TcpStream as TokioTcpStream};

/// Inner stream type: plain TCP or TLS-wrapped (when tls feature and tls_cert_dir set).
#[derive(Debug)]
enum TcpOrTlsStream {
    Plain(TokioTcpStream),
    #[cfg(feature = "tls")]
    Tls(tokio_rustls::client::TlsStream<TokioTcpStream>),
}

#[cfg(feature = "tls")]
impl tokio::io::AsyncRead for TcpOrTlsStream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            TcpOrTlsStream::Plain(s) => std::pin::Pin::new(s).poll_read(cx, buf),
            TcpOrTlsStream::Tls(s) => std::pin::Pin::new(s).poll_read(cx, buf),
        }
    }
}

#[cfg(feature = "tls")]
impl tokio::io::AsyncWrite for TcpOrTlsStream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match self.get_mut() {
            TcpOrTlsStream::Plain(s) => std::pin::Pin::new(s).poll_write(cx, buf),
            TcpOrTlsStream::Tls(s) => std::pin::Pin::new(s).poll_write(cx, buf),
        }
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            TcpOrTlsStream::Plain(s) => std::pin::Pin::new(s).poll_flush(cx),
            TcpOrTlsStream::Tls(s) => std::pin::Pin::new(s).poll_flush(cx),
        }
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            TcpOrTlsStream::Plain(s) => std::pin::Pin::new(s).poll_shutdown(cx),
            TcpOrTlsStream::Tls(s) => std::pin::Pin::new(s).poll_shutdown(cx),
        }
    }
}

#[cfg(not(feature = "tls"))]
impl tokio::io::AsyncRead for TcpOrTlsStream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            TcpOrTlsStream::Plain(s) => std::pin::Pin::new(s).poll_read(cx, buf),
        }
    }
}

#[cfg(not(feature = "tls"))]
impl tokio::io::AsyncWrite for TcpOrTlsStream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match self.get_mut() {
            TcpOrTlsStream::Plain(s) => std::pin::Pin::new(s).poll_write(cx, buf),
        }
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            TcpOrTlsStream::Plain(s) => std::pin::Pin::new(s).poll_flush(cx),
        }
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            TcpOrTlsStream::Plain(s) => std::pin::Pin::new(s).poll_shutdown(cx),
        }
    }
}

/// Wraps tokio TcpStream (or TLS stream) to implement the transport Stream trait.
pub struct TcpStreamImpl {
    inner: tokio::sync::Mutex<TcpOrTlsStream>,
}

impl TcpStreamImpl {
    pub fn new(inner: TokioTcpStream) -> Self {
        Self {
            inner: tokio::sync::Mutex::new(TcpOrTlsStream::Plain(inner)),
        }
    }

    #[cfg(feature = "tls")]
    pub fn new_tls(inner: tokio_rustls::client::TlsStream<TokioTcpStream>) -> Self {
        Self {
            inner: tokio::sync::Mutex::new(TcpOrTlsStream::Tls(inner)),
        }
    }
}

impl AsyncRead for TcpStreamImpl {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let mut guard = match self.inner.try_lock() {
            Ok(g) => g,
            Err(_) => return std::task::Poll::Pending,
        };
        std::pin::Pin::new(&mut *guard).poll_read(cx, buf)
    }
}

impl AsyncWrite for TcpStreamImpl {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let mut guard = match self.inner.try_lock() {
            Ok(g) => g,
            Err(_) => return std::task::Poll::Pending,
        };
        std::pin::Pin::new(&mut *guard).poll_write(cx, buf)
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let mut guard = match self.inner.try_lock() {
            Ok(g) => g,
            Err(_) => return std::task::Poll::Pending,
        };
        std::pin::Pin::new(&mut *guard).poll_flush(cx)
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let mut guard = match self.inner.try_lock() {
            Ok(g) => g,
            Err(_) => return std::task::Poll::Pending,
        };
        std::pin::Pin::new(&mut *guard).poll_shutdown(cx)
    }
}

#[async_trait]
impl Stream for TcpStreamImpl {
    async fn shutdown(&self) -> Result<(), TransferError> {
        let mut guard = self.inner.lock().await;
        match &mut *guard {
            TcpOrTlsStream::Plain(s) => s
                .shutdown()
                .await
                .map_err(|e| TransferError::NetworkError(format!("Stream shutdown: {}", e))),
            #[cfg(feature = "tls")]
            TcpOrTlsStream::Tls(s) => s
                .shutdown()
                .await
                .map_err(|e| TransferError::NetworkError(format!("TLS stream shutdown: {}", e))),
        }
    }
}

/// TCP connection: one stream per connection. Stream is taken on first open_stream or accept_stream.
pub struct TcpConnection {
    stream: std::sync::Mutex<Option<TcpStreamImpl>>,
    platform: Platform,
}

#[async_trait]
impl Connection for TcpConnection {
    async fn open_stream(&self) -> Result<Box<dyn Stream>, TransferError> {
        let mut guard = self.stream.lock().map_err(|e| {
            TransferError::ProtocolError(format!("Connection lock: {}", e))
        })?;
        let s = guard.take().ok_or_else(|| {
            TransferError::ProtocolError("TCP connection: stream already taken".to_string())
        })?;
        Ok(Box::new(s))
    }
    async fn accept_stream(&self) -> Result<Box<dyn Stream>, TransferError> {
        let mut guard = self.stream.lock().map_err(|e| {
            TransferError::ProtocolError(format!("Connection lock: {}", e))
        })?;
        let s = guard.take().ok_or_else(|| {
            TransferError::ProtocolError("TCP connection: stream already taken".to_string())
        })?;
        Ok(Box::new(s))
    }
    fn platform(&self) -> Platform {
        self.platform
    }
}

/// TCP listener wrapper.
pub struct TcpListenerImpl {
    listener: TokioTcpListener,
    platform: Platform,
}

#[async_trait]
impl Listener for TcpListenerImpl {
    async fn accept(&self) -> Result<Box<dyn Connection>, TransferError> {
        let (stream, _) = self
            .listener
            .accept()
            .await
            .map_err(|e| TransferError::NetworkError(format!("Accept: {}", e)))?;
        Ok(Box::new(TcpConnection {
            stream: std::sync::Mutex::new(Some(TcpStreamImpl::new(stream))),
            platform: self.platform,
        }))
    }
}

/// TCP transport. Implements Transport for use with create_transport() when QUIC is added.
pub struct TcpTransport {
    config: TransferConfig,
    #[cfg(feature = "tls")]
    client_config: Option<std::sync::Arc<rustls::ClientConfig>>,
}

impl TcpTransport {
    pub fn new(config: TransferConfig) -> Self {
        #[cfg(feature = "tls")]
        let client_config = config.tls_cert_dir.as_ref().and_then(|dir| {
            crate::tls::client_config_from_dir(dir).ok()
        });
        Self {
            config,
            #[cfg(feature = "tls")]
            client_config,
        }
    }

    #[cfg(feature = "tls")]
    fn server_name(addr: std::net::SocketAddr) -> Result<rustls::pki_types::ServerName<'static>, TransferError> {
        if addr.ip().is_loopback() {
            rustls::pki_types::ServerName::try_from("localhost".to_string())
                .map_err(|e| TransferError::ProtocolError(format!("ServerName: {}", e)))
        } else {
            Ok(rustls::pki_types::ServerName::from(addr.ip()))
        }
    }

    fn platform() -> Platform {
        if cfg!(target_os = "linux") {
            Platform::Linux
        } else if cfg!(target_os = "windows") {
            Platform::Windows
        } else {
            Platform::Unknown
        }
    }
}

fn configure_tcp_socket(
    socket: &Socket,
    send_buf: Option<usize>,
    recv_buf: Option<usize>,
) -> Result<(), TransferError> {
    if let Some(s) = send_buf {
        socket
            .set_send_buffer_size(s)
            .map_err(|e| TransferError::NetworkError(format!("SO_SNDBUF: {}", e)))?;
    }
    if let Some(s) = recv_buf {
        socket
            .set_recv_buffer_size(s)
            .map_err(|e| TransferError::NetworkError(format!("SO_RCVBUF: {}", e)))?;
    }
    socket
        .set_nodelay(true)
        .map_err(|e| TransferError::NetworkError(format!("TCP_NODELAY: {}", e)))?;
    Ok(())
}

/// Opens TCP data streams by connecting to base_port+1, base_port+2, ... Coordinator calls open_stream() on demand.
pub struct TcpStreamOpener {
    addr: SocketAddr,
    next_port: AtomicU16,
    max: usize,
    config: TransferConfig,
    #[cfg(feature = "tls")]
    client_config: Option<std::sync::Arc<rustls::ClientConfig>>,
}

#[async_trait]
impl StreamOpener for TcpStreamOpener {
    async fn open_stream(&self) -> Result<Box<dyn Stream>, TransferError> {
        let port = self.next_port.fetch_add(1, Ordering::SeqCst);
        let data_addr = SocketAddr::new(self.addr.ip(), port);
        let socket = Socket::new(Domain::IPV4, Type::STREAM, None)
            .map_err(|e| TransferError::NetworkError(format!("Socket: {}", e)))?;
        configure_tcp_socket(
            &socket,
            self.config.socket_send_buffer_size,
            self.config.socket_recv_buffer_size,
        )?;
        socket
            .connect(&data_addr.into())
            .map_err(|e| TransferError::NetworkError(format!("Connect: {}", e)))?;
        socket
            .set_nonblocking(true)
            .map_err(|e| TransferError::NetworkError(format!("Nonblocking: {}", e)))?;
        let std_stream = std::net::TcpStream::from(socket);
        let stream = TokioTcpStream::from_std(std_stream)
            .map_err(|e| TransferError::NetworkError(format!("Tokio stream: {}", e)))?;
        #[cfg(feature = "tls")]
        let stream = {
            if let Some(ref cfg) = self.client_config {
                let name = if data_addr.ip().is_loopback() {
                    rustls::pki_types::ServerName::try_from("localhost".to_string())
                        .map_err(|e| TransferError::ProtocolError(format!("ServerName: {}", e)))?
                } else {
                    rustls::pki_types::ServerName::from(data_addr.ip())
                };
                let connector = tokio_rustls::TlsConnector::from(cfg.clone());
                let tls_stream = connector
                    .connect(name, stream)
                    .await
                    .map_err(|e| TransferError::NetworkError(format!("TLS connect data: {}", e)))?;
                TcpStreamImpl::new_tls(tls_stream)
            } else {
                TcpStreamImpl::new(stream)
            }
        };
        #[cfg(not(feature = "tls"))]
        let stream = TcpStreamImpl::new(stream);
        Ok(Box::new(stream))
    }

    fn max_streams(&self) -> usize {
        self.max
    }
}

#[async_trait]
impl Transport for TcpTransport {
    async fn connect(&self, addr: SocketAddr) -> Result<Box<dyn Connection>, TransferError> {
        let socket = Socket::new(Domain::IPV4, Type::STREAM, None)
            .map_err(|e| TransferError::NetworkError(format!("Socket: {}", e)))?;
        configure_tcp_socket(
            &socket,
            self.config.socket_send_buffer_size,
            self.config.socket_recv_buffer_size,
        )?;
        socket
            .connect(&addr.into())
            .map_err(|e| TransferError::NetworkError(format!("Connect: {}", e)))?;
        socket
            .set_nonblocking(true)
            .map_err(|e| TransferError::NetworkError(format!("Nonblocking: {}", e)))?;
        let std_stream = std::net::TcpStream::from(socket);
        let stream = TokioTcpStream::from_std(std_stream)
            .map_err(|e| TransferError::NetworkError(format!("Tokio stream: {}", e)))?;
        #[cfg(feature = "tls")]
        let stream = {
            if let Some(ref cfg) = self.client_config {
                let name = Self::server_name(addr)?;
                let connector = tokio_rustls::TlsConnector::from(cfg.clone());
                let tls_stream = connector
                    .connect(name, stream)
                    .await
                    .map_err(|e| TransferError::NetworkError(format!("TLS connect: {}", e)))?;
                TcpStreamImpl::new_tls(tls_stream)
            } else {
                TcpStreamImpl::new(stream)
            }
        };
        #[cfg(not(feature = "tls"))]
        let stream = TcpStreamImpl::new(stream);
        Ok(Box::new(TcpConnection {
            stream: std::sync::Mutex::new(Some(stream)),
            platform: Self::platform(),
        }))
    }

    async fn listen(&self, addr: SocketAddr) -> Result<Box<dyn Listener>, TransferError> {
        let socket = Socket::new(Domain::IPV4, Type::STREAM, None)
            .map_err(|e| TransferError::NetworkError(format!("Socket: {}", e)))?;
        configure_tcp_socket(
            &socket,
            self.config.socket_send_buffer_size,
            self.config.socket_recv_buffer_size,
        )?;
        socket
            .bind(&addr.into())
            .map_err(|e| TransferError::NetworkError(format!("Bind: {}", e)))?;
        socket
            .listen(128)
            .map_err(|e| TransferError::NetworkError(format!("Listen: {}", e)))?;
        socket
            .set_nonblocking(true)
            .map_err(|e| TransferError::NetworkError(format!("Nonblocking: {}", e)))?;
        let std_listener = std::net::TcpListener::from(socket);
        let listener = TokioTcpListener::from_std(std_listener)
            .map_err(|e| TransferError::NetworkError(format!("Tokio listener: {}", e)))?;
        Ok(Box::new(TcpListenerImpl {
            listener,
            platform: Self::platform(),
        }))
    }

    async fn connect_for_transfer(
        &self,
        addr: SocketAddr,
        _num_streams: usize,
        max_streams: usize,
    ) -> Result<(TransferMetaChannels, Arc<dyn StreamOpener>), TransferError> {
        let socket = Socket::new(Domain::IPV4, Type::STREAM, None)
            .map_err(|e| TransferError::NetworkError(format!("Socket: {}", e)))?;
        configure_tcp_socket(
            &socket,
            self.config.socket_send_buffer_size,
            self.config.socket_recv_buffer_size,
        )?;
        socket
            .connect(&addr.into())
            .map_err(|e| TransferError::NetworkError(format!("Connect: {}", e)))?;
        socket
            .set_nonblocking(true)
            .map_err(|e| TransferError::NetworkError(format!("Nonblocking: {}", e)))?;
        let std_stream = std::net::TcpStream::from(socket);
        let meta_stream = TokioTcpStream::from_std(std_stream)
            .map_err(|e| TransferError::NetworkError(format!("Tokio stream: {}", e)))?;
        #[cfg(feature = "tls")]
        let meta = {
            if let Some(ref cfg) = self.client_config {
                let name = Self::server_name(addr)?;
                let connector = tokio_rustls::TlsConnector::from(cfg.clone());
                let tls_stream = connector
                    .connect(name, meta_stream)
                    .await
                    .map_err(|e| TransferError::NetworkError(format!("TLS connect metadata: {}", e)))?;
                let (r, w) = tokio::io::split(tls_stream);
                TransferMetaChannels {
                    reader: Box::new(r),
                    writer: Box::new(w),
                }
            } else {
                let (r, w) = tokio::io::split(meta_stream);
                TransferMetaChannels {
                    reader: Box::new(r),
                    writer: Box::new(w),
                }
            }
        };
        #[cfg(not(feature = "tls"))]
        let (reader, writer) = tokio::io::split(meta_stream);
        #[cfg(not(feature = "tls"))]
        let meta = TransferMetaChannels {
            reader: Box::new(reader),
            writer: Box::new(writer),
        };
        let opener = TcpStreamOpener {
            addr,
            next_port: AtomicU16::new(addr.port() + 1),
            max: max_streams,
            config: self.config.clone(),
            #[cfg(feature = "tls")]
            client_config: self.client_config.clone(),
        };
        Ok((meta, Arc::new(opener)))
    }

    async fn open_data_connections(
        &self,
        addr: SocketAddr,
        num: usize,
    ) -> Result<Vec<Box<dyn Stream>>, TransferError> {
        let base_port = addr.port();
        let ip = addr.ip();
        let send_buf = self.config.socket_send_buffer_size;
        let recv_buf = self.config.socket_recv_buffer_size;
        let streams: Vec<TokioTcpStream> = tokio::task::spawn_blocking(move || {
            let mut out = Vec::with_capacity(num);
            for i in 0..num {
                let data_addr = SocketAddr::new(ip, base_port + 1 + i as u16);
                let socket = Socket::new(Domain::IPV4, Type::STREAM, None)
                    .map_err(|e| TransferError::NetworkError(format!("Socket: {}", e)))?;
                configure_tcp_socket(&socket, send_buf, recv_buf)?;
                socket
                    .connect(&data_addr.into())
                    .map_err(|e| TransferError::NetworkError(format!("Connect: {}", e)))?;
                socket
                    .set_nonblocking(true)
                    .map_err(|e| TransferError::NetworkError(format!("Nonblocking: {}", e)))?;
                let std_stream = std::net::TcpStream::from(socket);
                let stream = TokioTcpStream::from_std(std_stream)
                    .map_err(|e| TransferError::NetworkError(format!("Tokio stream: {}", e)))?;
                out.push(stream);
            }
            Ok::<_, TransferError>(out)
        })
        .await
        .map_err(|e| TransferError::NetworkError(format!("TCP connect task panicked: {:?}", e)))??;
        let data_streams = streams
            .into_iter()
            .map(|s| Box::new(TcpStreamImpl::new(s)) as Box<dyn Stream>)
            .collect();
        Ok(data_streams)
    }
}

/// Create a transport: QUIC (if available and not force_tcp) or TCP.
/// Tries QuicTransport::probe() first when force_tcp is false; on failure or if force_tcp is true, returns TcpTransport.
pub async fn create_transport(
    config: &TransferConfig,
    force_tcp: bool,
) -> Box<dyn Transport> {
    if !force_tcp {
        if let Ok(quic) = crate::quinn_transport::QuicTransport::probe().await {
            return Box::new(quic);
        }
    }
    Box::new(TcpTransport::new(config.clone()))
}
