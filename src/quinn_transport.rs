//! QUIC transport using quinn. Implements Transport/Connection/Stream.

use crate::error::TransferError;
use crate::transport::{Connection, Platform, Stream, TransferMetaChannels, Transport};
use async_trait::async_trait;
use quinn::{ClientConfig, Connection as QuinnConnection, Endpoint, RecvStream, SendStream};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// Wraps quinn (SendStream, RecvStream) as a single type implementing AsyncRead + AsyncWrite.
struct QuinnStream {
    send: tokio::sync::Mutex<SendStream>,
    recv: tokio::sync::Mutex<RecvStream>,
}

impl QuinnStream {
    fn new(send: SendStream, recv: RecvStream) -> Self {
        Self {
            send: tokio::sync::Mutex::new(send),
            recv: tokio::sync::Mutex::new(recv),
        }
    }
}

impl Unpin for QuinnStream {}

impl AsyncRead for QuinnStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let mut guard = match self.recv.try_lock() {
            Ok(g) => g,
            Err(_) => return Poll::Pending,
        };
        Pin::new(&mut *guard).poll_read(cx, buf)
    }
}

impl AsyncWrite for QuinnStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let mut guard = match self.send.try_lock() {
            Ok(g) => g,
            Err(_) => return Poll::Pending,
        };
        Pin::new(&mut *guard)
            .poll_write(cx, buf)
            .map(|r| r.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)))
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let mut guard = match self.send.try_lock() {
            Ok(g) => g,
            Err(_) => return Poll::Pending,
        };
        Pin::new(&mut *guard)
            .poll_flush(cx)
            .map(|r| r.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)))
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let mut guard = match self.send.try_lock() {
            Ok(g) => g,
            Err(_) => return Poll::Pending,
        };
        Pin::new(&mut *guard)
            .poll_shutdown(cx)
            .map(|r| r.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)))
    }
}

#[async_trait]
impl Stream for QuinnStream {
    async fn shutdown(&self) -> Result<(), TransferError> {
        let mut send = self.send.lock().await;
        send.finish()
            .map_err(|e| TransferError::NetworkError(format!("QUIC stream finish: {}", e)))?;
        Ok(())
    }
}

/// QUIC connection (quinn): can open and accept bidirectional streams.
struct QuinnConnectionImpl {
    conn: QuinnConnection,
    platform: Platform,
}

#[async_trait]
impl Connection for QuinnConnectionImpl {
    async fn open_stream(&self) -> Result<Box<dyn Stream>, TransferError> {
        let (send, recv) = self
            .conn
            .open_bi()
            .await
            .map_err(|e| TransferError::NetworkError(format!("QUIC open_bi: {}", e)))?;
        Ok(Box::new(QuinnStream::new(send, recv)))
    }
    async fn accept_stream(&self) -> Result<Box<dyn Stream>, TransferError> {
        let (send, recv) = self
            .conn
            .accept_bi()
            .await
            .map_err(|e| TransferError::NetworkError(format!("QUIC accept_bi: {}", e)))?;
        Ok(Box::new(QuinnStream::new(send, recv)))
    }
    fn platform(&self) -> Platform {
        self.platform
    }
}

/// QUIC listener: accepts one connection at a time (quinn endpoint accept).
struct QuinnListenerImpl {
    endpoint: Endpoint,
    platform: Platform,
}

#[async_trait]
impl crate::transport::Listener for QuinnListenerImpl {
    async fn accept(&self) -> Result<Box<dyn Connection>, TransferError> {
        let incoming = self
            .endpoint
            .accept()
            .await
            .ok_or_else(|| TransferError::ProtocolError("QUIC endpoint closed".to_string()))?;
        let conn = incoming
            .await
            .map_err(|e| TransferError::NetworkError(format!("QUIC accept: {}", e)))?;
        Ok(Box::new(QuinnConnectionImpl {
            conn,
            platform: self.platform,
        }))
    }
}

/// QUIC transport using quinn. Uses TLS with a dummy verifier for connection (insecure; for same-host or testing).
pub struct QuicTransport {
    platform: Platform,
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

impl QuicTransport {
    /// Create a QUIC transport. Does not bind yet; connect/listen will create endpoints.
    pub fn new() -> Self {
        Self {
            platform: platform(),
        }
    }

    /// Probe: try to create a client endpoint (bind UDP). If this fails (e.g. UDP blocked), caller should use TCP.
    pub async fn probe() -> Result<Self, TransferError> {
        let addr: SocketAddr = "0.0.0.0:0".parse().map_err(|_| {
            TransferError::ProtocolError("Invalid probe address".to_string())
        })?;
        let _ = Endpoint::client(addr).map_err(|e| {
            TransferError::NetworkError(format!("QUIC probe (UDP bind): {}", e))
        })?;
        Ok(Self::new())
    }

    fn make_client_config() -> Result<ClientConfig, TransferError> {
        use quinn::crypto::rustls::QuicClientConfig;
        use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerifier, ServerCertVerified};
        use rustls::pki_types::ServerName;
        use rustls::pki_types::UnixTime;
        use std::sync::Arc;

        #[derive(Debug)]
        struct SkipVerifier;
        impl ServerCertVerifier for SkipVerifier {
            fn verify_server_cert(
                &self,
                _end_entity: &rustls::pki_types::CertificateDer<'_>,
                _intermediates: &[rustls::pki_types::CertificateDer<'_>],
                _server_name: &ServerName<'_>,
                _ocsp: &[u8],
                _now: UnixTime,
            ) -> Result<ServerCertVerified, rustls::Error> {
                Ok(ServerCertVerified::assertion())
            }
            fn verify_tls12_signature(
                &self,
                _message: &[u8],
                _cert: &rustls::pki_types::CertificateDer<'_>,
                _dss: &rustls::DigitallySignedStruct,
            ) -> Result<HandshakeSignatureValid, rustls::Error> {
                Ok(HandshakeSignatureValid::assertion())
            }
            fn verify_tls13_signature(
                &self,
                _message: &[u8],
                _cert: &rustls::pki_types::CertificateDer<'_>,
                _dss: &rustls::DigitallySignedStruct,
            ) -> Result<HandshakeSignatureValid, rustls::Error> {
                Ok(HandshakeSignatureValid::assertion())
            }
            fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
                rustls::crypto::ring::default_provider()
                    .signature_verification_algorithms
                    .supported_schemes()
            }
        }

        let tls = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(SkipVerifier))
            .with_no_client_auth();
        let quic_cfg = QuicClientConfig::try_from(Arc::new(tls))
            .map_err(|e| TransferError::ProtocolError(format!("QUIC client config: {}", e)))?;
        Ok(ClientConfig::new(Arc::new(quic_cfg)))
    }
}

#[async_trait]
impl Transport for QuicTransport {
    async fn connect(&self, addr: SocketAddr) -> Result<Box<dyn Connection>, TransferError> {
        let mut endpoint = Endpoint::client("0.0.0.0:0".parse().unwrap())
            .map_err(|e| TransferError::NetworkError(format!("QUIC client endpoint: {}", e)))?;
        endpoint.set_default_client_config(Self::make_client_config()?);
        let server_name = addr.ip().to_string();
        let connecting = endpoint
            .connect(addr, &server_name)
            .map_err(|e| TransferError::NetworkError(format!("QUIC connect: {}", e)))?;
        let conn = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            connecting,
        )
        .await
        .map_err(|_| TransferError::NetworkError("QUIC handshake timeout".to_string()))?
        .map_err(|e| TransferError::NetworkError(format!("QUIC handshake: {}", e)))?;
        std::mem::forget(endpoint);
        Ok(Box::new(QuinnConnectionImpl {
            conn,
            platform: self.platform,
        }))
    }

    async fn listen(&self, addr: SocketAddr) -> Result<Box<dyn crate::transport::Listener>, TransferError> {
        use rustls::pki_types::PrivatePkcs8KeyDer;
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()])
            .map_err(|e| TransferError::ProtocolError(format!("QUIC cert: {}", e)))?;
        let cert_der = rustls::pki_types::CertificateDer::from(cert.serialize_der().unwrap());
        let key = PrivatePkcs8KeyDer::from(cert.serialize_private_key_der());
        let mut server_config = quinn::ServerConfig::with_single_cert(vec![cert_der], key.into())
            .map_err(|e| TransferError::ProtocolError(format!("QUIC server config: {}", e)))?;
        let transport = Arc::get_mut(&mut server_config.transport).unwrap();
        transport.max_concurrent_uni_streams(0u8.into());
        let endpoint = Endpoint::server(server_config, addr)
            .map_err(|e| TransferError::NetworkError(format!("QUIC server bind: {}", e)))?;
        Ok(Box::new(QuinnListenerImpl {
            endpoint,
            platform: self.platform,
        }))
    }

    async fn connect_for_transfer(
        &self,
        addr: SocketAddr,
        num_data_streams: usize,
    ) -> Result<(TransferMetaChannels, Vec<Box<dyn Stream>>), TransferError> {
        let mut endpoint = Endpoint::client("0.0.0.0:0".parse().unwrap())
            .map_err(|e| TransferError::NetworkError(format!("QUIC client endpoint: {}", e)))?;
        endpoint.set_default_client_config(Self::make_client_config()?);
        let server_name = addr.ip().to_string();
        let connecting = endpoint
            .connect(addr, &server_name)
            .map_err(|e| TransferError::NetworkError(format!("QUIC connect: {}", e)))?;
        let conn = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            connecting,
        )
        .await
        .map_err(|_| TransferError::NetworkError("QUIC handshake timeout".to_string()))?
        .map_err(|e| TransferError::NetworkError(format!("QUIC handshake: {}", e)))?;
        std::mem::forget(endpoint);

        let (meta_send, meta_recv) = conn
            .open_bi()
            .await
            .map_err(|e| TransferError::NetworkError(format!("QUIC open_bi (meta): {}", e)))?;
        let meta_stream = QuinnStream::new(meta_send, meta_recv);
        let (reader, writer) = tokio::io::split(meta_stream);
        let meta = TransferMetaChannels {
            reader: Box::new(reader),
            writer: Box::new(writer),
        };

        let mut data_streams: Vec<Box<dyn Stream>> = Vec::with_capacity(num_data_streams);
        for _ in 0..num_data_streams {
            let (send, recv) = conn
                .open_bi()
                .await
                .map_err(|e| TransferError::NetworkError(format!("QUIC open_bi (data): {}", e)))?;
            data_streams.push(Box::new(QuinnStream::new(send, recv)));
        }

        std::mem::forget(conn);
        Ok((meta, data_streams))
    }
}
