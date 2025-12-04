//! QUIC transport layer for file transfers.
//!
//! This module provides QUIC connection setup, certificate handling, and
//! stream management for both client and server operations.

use crate::error::TransferError;
use quinn::{ClientConfig, Endpoint, ServerConfig};
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};

/// Initialize rustls crypto provider (called once on first use).
static CRYPTO_INIT: OnceLock<()> = OnceLock::new();

fn ensure_crypto_provider() {
    CRYPTO_INIT.get_or_init(|| {
        // Install default crypto provider for rustls (using ring)
        rustls::crypto::ring::default_provider()
            .install_default()
            .expect("Failed to install rustls crypto provider");
    });
}

/// Create a QUIC client endpoint with default configuration.
pub fn create_client_endpoint(
    bind_addr: SocketAddr,
) -> Result<Endpoint, TransferError> {
    let client_config = create_client_config()?;
    let mut endpoint = Endpoint::client(bind_addr)?;
    endpoint.set_default_client_config(client_config);
    Ok(endpoint)
}

/// Create a QUIC server endpoint with default configuration.
pub fn create_server_endpoint(
    bind_addr: SocketAddr,
) -> Result<Endpoint, TransferError> {
    let server_config = create_server_config()?;
    let endpoint = Endpoint::server(server_config, bind_addr)?;
    Ok(endpoint)
}

/// Skip server certificate verification (development only).
///
/// WARNING: This allows any server to impersonate your server.
/// Only use in development environments.
#[derive(Debug)]
struct SkipServerVerification;

impl rustls::client::danger::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }
    
    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    
    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::ED25519,
        ]
    }
}

/// Create client configuration with certificate validation.
///
/// In development, this skips certificate verification to allow self-signed certs.
/// In production, load proper CA certificates for verification.
fn create_client_config() -> Result<ClientConfig, TransferError> {
    ensure_crypto_provider();
    // For development: skip certificate verification
    // For production: load system CA certificates
    let rustls_client_config = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(SkipServerVerification))
        .with_no_client_auth();

    // Convert to quinn client config
    let quic_client_config = quinn::crypto::rustls::QuicClientConfig::try_from(Arc::new(rustls_client_config))
        .map_err(|e| TransferError::ProtocolError(format!("Failed to create quic client config: {:?}", e)))?;

    let mut config = ClientConfig::new(Arc::new(quic_client_config));
    
    // Configure transport parameters for high throughput
    let mut transport = quinn::TransportConfig::default();
    transport.max_concurrent_bidi_streams(1000u32.into());
    transport.max_concurrent_uni_streams(1000u32.into());
    transport.initial_mtu(1500u16);
    // Increase send/receive windows for better throughput
    transport.send_window(16 * 1024 * 1024);
    transport.receive_window((16 * 1024 * 1024u32).into());
    transport.stream_receive_window((8 * 1024 * 1024u32).into());
    config.transport_config(Arc::new(transport));

    Ok(config)
}

/// Create server configuration with self-signed certificate.
fn create_server_config() -> Result<ServerConfig, TransferError> {
    ensure_crypto_provider();
    // Generate self-signed certificate for development
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()])
        .map_err(|e| TransferError::ProtocolError(format!("Failed to generate cert: {}", e)))?;
    
    let cert_der = cert.cert.der();
    let key_der = cert.key_pair.serialize_der();
    
    let cert_chain = vec![rustls::pki_types::CertificateDer::from(cert_der.to_vec())];
    let key = rustls::pki_types::PrivateKeyDer::Pkcs8(
        rustls::pki_types::PrivatePkcs8KeyDer::from(key_der)
    );

    let rustls_server_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert_chain, key)
        .map_err(|e| TransferError::ProtocolError(format!("Failed to create server config: {}", e)))?;

    let quic_server_config = quinn::crypto::rustls::QuicServerConfig::try_from(Arc::new(rustls_server_config))
        .map_err(|e| TransferError::ProtocolError(format!("Failed to create quic server config: {:?}", e)))?;

    let mut config = ServerConfig::with_crypto(Arc::new(quic_server_config));
    
    // Configure transport parameters for high throughput
    let mut transport = quinn::TransportConfig::default();
    transport.max_concurrent_bidi_streams(1000u32.into());
    transport.max_concurrent_uni_streams(1000u32.into());
    transport.initial_mtu(1500u16);
    // Increase send/receive windows for better throughput
    transport.send_window(16 * 1024 * 1024);
    transport.receive_window((16 * 1024 * 1024u32).into());
    transport.stream_receive_window((8 * 1024 * 1024u32).into());
    config.transport_config(Arc::new(transport));

    Ok(config)
}
