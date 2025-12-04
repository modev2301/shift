//! QUIC transport layer for file transfers.
//!
//! This module provides QUIC connection setup, certificate handling, and
//! stream management for both client and server operations.

use crate::error::TransferError;
use quinn::{ClientConfig, Endpoint, ServerConfig};
use std::net::SocketAddr;
use std::sync::Arc;

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

/// Create client configuration with certificate validation.
fn create_client_config() -> Result<ClientConfig, TransferError> {
    // Build rustls client config
    // For development, use empty root store to allow self-signed certs
    // In production, load system certificates properly
    let root_store = rustls::RootCertStore::empty();

    let rustls_client_config = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
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
    config.transport_config(Arc::new(transport));

    Ok(config)
}

/// Create server configuration with self-signed certificate.
fn create_server_config() -> Result<ServerConfig, TransferError> {
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
    config.transport_config(Arc::new(transport));

    Ok(config)
}
