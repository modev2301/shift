//! Transport abstraction: TCP implements today, QUIC implements same traits later.
//! Nothing above this layer changes when swapping transport.

use crate::error::TransferError;
use async_trait::async_trait;
use std::net::SocketAddr;
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

/// Transport: connect (client) or listen (server). TCP and QUIC implement this.
#[async_trait]
pub trait Transport: Send + Sync {
    async fn connect(&self, addr: SocketAddr) -> Result<Box<dyn Connection>, TransferError>;
    async fn listen(&self, addr: SocketAddr) -> Result<Box<dyn Listener>, TransferError>;

    /// Establish channels for file transfer: one metadata (split into reader/writer) and N data streams.
    async fn connect_for_transfer(
        &self,
        addr: SocketAddr,
        num_data_streams: usize,
    ) -> Result<(TransferMetaChannels, Vec<Box<dyn Stream>>), TransferError>;
}

// -----------------------------------------------------------------------------
// TCP implementation
// -----------------------------------------------------------------------------

use socket2::{Domain, Socket, Type};
use tokio::net::{TcpListener as TokioTcpListener, TcpStream as TokioTcpStream};

use crate::base::TransferConfig;

/// Wraps tokio TcpStream to implement the transport Stream trait (AsyncRead + AsyncWrite + shutdown).
pub struct TcpStreamImpl {
    inner: tokio::sync::Mutex<tokio::net::TcpStream>,
}

impl TcpStreamImpl {
    pub fn new(inner: tokio::net::TcpStream) -> Self {
        Self {
            inner: tokio::sync::Mutex::new(inner),
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
        guard
            .shutdown()
            .await
            .map_err(|e| TransferError::NetworkError(format!("Stream shutdown: {}", e)))
    }
}

/// TCP connection: one stream per connection. Stream is taken on first open_stream or accept_stream.
pub struct TcpConnection {
    stream: std::sync::Mutex<Option<tokio::net::TcpStream>>,
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
        Ok(Box::new(TcpStreamImpl::new(s)))
    }
    async fn accept_stream(&self) -> Result<Box<dyn Stream>, TransferError> {
        let mut guard = self.stream.lock().map_err(|e| {
            TransferError::ProtocolError(format!("Connection lock: {}", e))
        })?;
        let s = guard.take().ok_or_else(|| {
            TransferError::ProtocolError("TCP connection: stream already taken".to_string())
        })?;
        Ok(Box::new(TcpStreamImpl::new(s)))
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
            stream: std::sync::Mutex::new(Some(stream)),
            platform: self.platform,
        }))
    }
}

/// TCP transport. Implements Transport for use with create_transport() when QUIC is added.
pub struct TcpTransport {
    config: TransferConfig,
}

impl TcpTransport {
    pub fn new(config: TransferConfig) -> Self {
        Self { config }
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
        num_data_streams: usize,
    ) -> Result<(TransferMetaChannels, Vec<Box<dyn Stream>>), TransferError> {
        let connect_one = |target: SocketAddr,
                          send_buf: Option<usize>,
                          recv_buf: Option<usize>|
         -> Result<TokioTcpStream, TransferError> {
            let socket = Socket::new(Domain::IPV4, Type::STREAM, None)
                .map_err(|e| TransferError::NetworkError(format!("Socket: {}", e)))?;
            configure_tcp_socket(&socket, send_buf, recv_buf)?;
            socket
                .connect(&target.into())
                .map_err(|e| TransferError::NetworkError(format!("Connect: {}", e)))?;
            socket
                .set_nonblocking(true)
                .map_err(|e| TransferError::NetworkError(format!("Nonblocking: {}", e)))?;
            let std_stream = std::net::TcpStream::from(socket);
            TokioTcpStream::from_std(std_stream)
                .map_err(|e| TransferError::NetworkError(format!("Tokio stream: {}", e)))
        };

        let meta_stream = connect_one(addr, self.config.socket_send_buffer_size, self.config.socket_recv_buffer_size)?;
        let (reader, writer) = tokio::io::split(meta_stream);
        let meta = TransferMetaChannels {
            reader: Box::new(reader),
            writer: Box::new(writer),
        };

        let base_port = addr.port();
        let ip = addr.ip();
        let send_buf = self.config.socket_send_buffer_size;
        let recv_buf = self.config.socket_recv_buffer_size;
        let mut join_handles = Vec::with_capacity(num_data_streams);
        for i in 0..num_data_streams {
            let data_addr = SocketAddr::new(ip, base_port + 1 + i as u16);
            let (sb, rb) = (send_buf, recv_buf);
            join_handles.push(tokio::task::spawn_blocking(move || connect_one(data_addr, sb, rb)));
        }
        let mut data_streams: Vec<Box<dyn Stream>> = Vec::with_capacity(num_data_streams);
        for h in join_handles {
            let stream = h.await.map_err(|e| {
                TransferError::NetworkError(format!("TCP connect task panicked: {:?}", e))
            })??;
            data_streams.push(Box::new(TcpStreamImpl::new(stream)));
        }

        Ok((meta, data_streams))
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
