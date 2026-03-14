//! TCP mutual TLS: cert generation (rcgen), server/client configs (rustls), optional wrapping of TCP streams.
//! Enabled with the `tls` feature.

use crate::error::TransferError;
use std::path::Path;
use std::sync::Arc;

#[cfg(feature = "tls")]
mod imp {
    use super::*;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};
    use rustls::server::WebPkiClientVerifier;
    use rustls::ClientConfig;
    use rustls::RootCertStore;
    use rustls::ServerConfig;
    use std::fs;
    use std::io::BufReader;

    fn ca_and_certs() -> Result<
        (
            rcgen::Certificate,
            (Vec<u8>, rcgen::Certificate), // (signed_der, cert with key)
            (Vec<u8>, rcgen::Certificate),
        ),
        TransferError,
    > {
        use rcgen::CertificateParams;

        let mut ca_params = CertificateParams::new(vec!["shift-ca".to_string()]);
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        ca_params.distinguished_name = rcgen::DistinguishedName::new();
        let ca_cert = rcgen::Certificate::from_params(ca_params)
            .map_err(|e| TransferError::ProtocolError(format!("CA cert: {}", e)))?;

        let server_params = CertificateParams::new(vec![
            "localhost".to_string(),
            "127.0.0.1".to_string(),
            "shift-server".to_string(),
        ]);
        let server_cert = rcgen::Certificate::from_params(server_params)
            .map_err(|e| TransferError::ProtocolError(format!("Server cert: {}", e)))?;
        let server_signed_der = server_cert.serialize_der_with_signer(&ca_cert)
            .map_err(|e| TransferError::ProtocolError(format!("Server sign: {}", e)))?;

        let client_params = CertificateParams::new(vec!["shift-client".to_string()]);
        let client_cert = rcgen::Certificate::from_params(client_params)
            .map_err(|e| TransferError::ProtocolError(format!("Client cert: {}", e)))?;
        let client_signed_der = client_cert.serialize_der_with_signer(&ca_cert)
            .map_err(|e| TransferError::ProtocolError(format!("Client sign: {}", e)))?;

        Ok((ca_cert, (server_signed_der, server_cert), (client_signed_der, client_cert)))
    }

    fn cert_der(der: &[u8]) -> CertificateDer<'static> {
        CertificateDer::from(der.to_vec())
    }

    fn key_der(cert: &rcgen::Certificate) -> PrivateKeyDer<'static> {
        PrivateKeyDer::try_from(cert.serialize_private_key_der()).unwrap()
    }

    fn der_to_pem(der: &[u8], label: &str) -> String {
        const TABLE: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut b64 = Vec::new();
        for chunk in der.chunks(3) {
            let n = (chunk[0] as usize) << 16
                | (chunk.get(1).copied().unwrap_or(0) as usize) << 8
                | chunk.get(2).copied().unwrap_or(0) as usize;
            b64.push(TABLE[(n >> 18) & 63]);
            b64.push(TABLE[(n >> 12) & 63]);
            b64.push(if chunk.len() > 1 { TABLE[(n >> 6) & 63] } else { b'=' });
            b64.push(if chunk.len() > 2 { TABLE[n & 63] } else { b'=' });
        }
        let line: String = b64.chunks(64).map(|c| std::str::from_utf8(c).unwrap()).collect::<Vec<_>>().join("\n");
        format!("-----BEGIN {}-----\n{}\n-----END {}-----", label, line, label)
    }

    /// Generate CA, server, and client certs and write PEM files to `dir`.
    pub fn keygen(dir: &Path) -> Result<(), TransferError> {
        let (ca_cert, (server_der, server_cert), (client_der, client_cert)) = ca_and_certs()?;
        fs::create_dir_all(dir).map_err(TransferError::Io)?;
        fs::write(dir.join("ca.pem"), ca_cert.serialize_pem().unwrap().as_bytes())?;
        fs::write(dir.join("server.pem"), der_to_pem(&server_der, "CERTIFICATE").as_bytes())?;
        fs::write(dir.join("server-key.pem"), server_cert.serialize_private_key_pem().as_bytes())?;
        fs::write(dir.join("client.pem"), der_to_pem(&client_der, "CERTIFICATE").as_bytes())?;
        fs::write(dir.join("client-key.pem"), client_cert.serialize_private_key_pem().as_bytes())?;
        Ok(())
    }

    fn load_pem_cert(path: &Path) -> Result<Vec<CertificateDer<'static>>, TransferError> {
        let data = fs::read(path)?;
        let mut reader = BufReader::new(data.as_slice());
        let certs = rustls_pemfile::certs(&mut reader)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| TransferError::ProtocolError(format!("PEM cert: {}", e)))?;
        Ok(certs.into_iter().map(CertificateDer::from).collect())
    }

    fn load_pem_key(path: &Path) -> Result<PrivateKeyDer<'static>, TransferError> {
        let data = fs::read(path)?;
        let mut reader = BufReader::new(data.as_slice());
        let key = rustls_pemfile::private_key(&mut reader)
            .map_err(|e| TransferError::ProtocolError(format!("PEM key: {}", e)))?
            .ok_or_else(|| TransferError::ProtocolError("No private key in file".to_string()))?;
        Ok(PrivateKeyDer::from(key))
    }

    /// Build server config with mutual TLS: server cert from dir, client certs verified by CA in dir.
    pub fn server_config_from_dir(dir: &Path) -> Result<Arc<ServerConfig>, TransferError> {
        let ca_certs = load_pem_cert(&dir.join("ca.pem"))?;
        let mut root_store = RootCertStore::empty();
        for cert in &ca_certs {
            root_store.add(cert.clone()).map_err(|e| {
                TransferError::ProtocolError(format!("CA cert add: {}", e))
            })?;
        }
        let client_verifier = WebPkiClientVerifier::builder(Arc::new(root_store))
            .build()
            .map_err(|e| TransferError::ProtocolError(format!("Client verifier: {}", e)))?;
        let server_certs = load_pem_cert(&dir.join("server.pem"))?;
        let server_key = load_pem_key(&dir.join("server-key.pem"))?;
        let config = ServerConfig::builder()
            .with_client_cert_verifier(client_verifier)
            .with_single_cert(server_certs, server_key)
            .map_err(|e| TransferError::ProtocolError(format!("ServerConfig: {}", e)))?;
        Ok(Arc::new(config))
    }

    /// Build client config with mutual TLS: client cert from dir, server verified by CA in dir.
    pub fn client_config_from_dir(dir: &Path) -> Result<Arc<ClientConfig>, TransferError> {
        let ca_certs = load_pem_cert(&dir.join("ca.pem"))?;
        let mut root_store = RootCertStore::empty();
        for cert in &ca_certs {
            root_store.add(cert.clone()).map_err(|e| {
                TransferError::ProtocolError(format!("CA cert add: {}", e))
            })?;
        }
        let client_certs = load_pem_cert(&dir.join("client.pem"))?;
        let client_key = load_pem_key(&dir.join("client-key.pem"))?;
        let config = ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_client_auth_cert(client_certs, client_key)
            .map_err(|e| TransferError::ProtocolError(format!("ClientConfig: {}", e)))?;
        Ok(Arc::new(config))
    }

    /// Generate in-memory server and client configs (for tests or when cert dir not used).
    pub fn generate_configs() -> Result<(Arc<ServerConfig>, Arc<ClientConfig>), TransferError> {
        let (ca_cert, (server_der, server_cert), (client_der, client_cert)) = ca_and_certs()?;
        let ca_der = ca_cert.serialize_der().map_err(|e| TransferError::ProtocolError(format!("CA serialize: {}", e)))?;
        let mut root_store_server = RootCertStore::empty();
        root_store_server.add(cert_der(&ca_der)).map_err(|e| TransferError::ProtocolError(format!("CA add: {}", e)))?;
        let client_verifier = WebPkiClientVerifier::builder(Arc::new(root_store_server))
            .build()
            .map_err(|e| TransferError::ProtocolError(format!("Client verifier: {}", e)))?;
        let mut root_store_client = RootCertStore::empty();
        root_store_client.add(cert_der(&ca_der)).map_err(|e| TransferError::ProtocolError(format!("CA add client: {}", e)))?;
        let server_config = ServerConfig::builder()
            .with_client_cert_verifier(client_verifier)
            .with_single_cert(vec![cert_der(&server_der)], key_der(&server_cert))
            .map_err(|e| TransferError::ProtocolError(format!("ServerConfig: {}", e)))?;
        let client_config = ClientConfig::builder()
            .with_root_certificates(root_store_client)
            .with_client_auth_cert(vec![cert_der(&client_der)], key_der(&client_cert))
            .map_err(|e| TransferError::ProtocolError(format!("ClientConfig: {}", e)))?;
        Ok((Arc::new(server_config), Arc::new(client_config)))
    }
}

#[cfg(feature = "tls")]
pub use imp::{client_config_from_dir, generate_configs, keygen, server_config_from_dir};

#[cfg(not(feature = "tls"))]
pub fn keygen(_dir: &Path) -> Result<(), TransferError> {
    Err(TransferError::ProtocolError(
        "TLS support not compiled in; build with --features tls".to_string(),
    ))
}
