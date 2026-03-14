//! TCP server implementation for parallel file transfer.
//!
//! The server listens on multiple ports: port 8080 for metadata,
//! and ports 8081-808N for parallel data connections.

use crate::base::cap_flags;
use crate::base::msg;
use crate::base::{apply_capabilities_to_config, capabilities_from_config, Capabilities, CAPABILITIES_WIRE_LEN};
use crate::error::TransferError;
use crate::base::TransferConfig;
use crate::base::FileRange;
use crate::integrity::{hash_file, hash_file_range_path, BLAKE3_LEN};
use crate::server_cache;
use crate::tcp_transfer::{configure_tcp_socket, receive_range_tcp};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::sync::mpsc as std_mpsc;
use socket2::{Domain, Socket, Type};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::error;

#[cfg(feature = "tls")]
use tokio_rustls::TlsAcceptor;

#[cfg(feature = "tls")]
enum MetaStream {
    Plain(tokio::net::TcpStream),
    Tls(tokio_rustls::server::TlsStream<tokio::net::TcpStream>),
}

#[cfg(feature = "tls")]
impl tokio::io::AsyncRead for MetaStream {
    fn poll_read(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>, buf: &mut tokio::io::ReadBuf<'_>) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            MetaStream::Plain(s) => std::pin::Pin::new(s).poll_read(cx, buf),
            MetaStream::Tls(s) => std::pin::Pin::new(s).poll_read(cx, buf),
        }
    }
}

#[cfg(feature = "tls")]
impl tokio::io::AsyncWrite for MetaStream {
    fn poll_write(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>, buf: &[u8]) -> std::task::Poll<std::io::Result<usize>> {
        match self.get_mut() {
            MetaStream::Plain(s) => std::pin::Pin::new(s).poll_write(cx, buf),
            MetaStream::Tls(s) => std::pin::Pin::new(s).poll_write(cx, buf),
        }
    }
    fn poll_flush(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            MetaStream::Plain(s) => std::pin::Pin::new(s).poll_flush(cx),
            MetaStream::Tls(s) => std::pin::Pin::new(s).poll_flush(cx),
        }
    }
    fn poll_shutdown(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            MetaStream::Plain(s) => std::pin::Pin::new(s).poll_shutdown(cx),
            MetaStream::Tls(s) => std::pin::Pin::new(s).poll_shutdown(cx),
        }
    }
}

/// TCP server that handles file transfers using parallel connections.
pub struct TcpServer {
    base_port: u16,
    output_dir: PathBuf,
    config: TransferConfig,
    completed_hashes: Arc<Mutex<HashMap<PathBuf, [u8; BLAKE3_LEN]>>>,
    persist_tx: Option<std_mpsc::Sender<(PathBuf, [u8; BLAKE3_LEN])>>,
    #[cfg(feature = "tls")]
    tls_acceptor: Option<TlsAcceptor>,
}

impl TcpServer {
    /// Create a new TCP server. When config.tls_cert_dir is set and build has `tls` feature, uses mutual TLS.
    /// Loads completed file hashes from ~/.shift/server_cache.db so --update skip persists across restarts.
    pub fn new(base_port: u16, output_dir: PathBuf, config: TransferConfig) -> Self {
        #[cfg(feature = "tls")]
        let tls_acceptor = config.tls_cert_dir.as_ref().and_then(|dir| {
            crate::tls::server_config_from_dir(dir)
                .map(|cfg| TlsAcceptor::from(cfg))
                .ok()
        });
        #[cfg(not(feature = "tls"))]
        let _ = config.tls_cert_dir.is_some(); // ignore if no tls feature
        let cache_path = server_cache::server_cache_path();
        let _ = server_cache::ensure_cache_dir(&cache_path);
        let (initial_hashes, persist_tx) = server_cache::load_and_spawn_persist(&cache_path);
        Self {
            base_port,
            output_dir,
            config,
            completed_hashes: Arc::new(Mutex::new(initial_hashes)),
            persist_tx,
            #[cfg(feature = "tls")]
            tls_acceptor,
        }
    }

    /// Run the server forever, accepting connections. If `base_port` is 0, binds to port 0 and sends the assigned port on `port_tx` when provided.
    pub async fn run_forever(&self, port_tx: Option<oneshot::Sender<u16>>) -> Result<(), TransferError> {
        let metadata_socket = Socket::new(Domain::IPV4, Type::STREAM, None)
            .map_err(|e| TransferError::NetworkError(format!("Failed to create socket: {}", e)))?;
        if let Some(size) = self.config.socket_send_buffer_size {
            metadata_socket.set_send_buffer_size(size)
                .map_err(|e| TransferError::NetworkError(format!("Failed to set SO_SNDBUF: {}", e)))?;
        }
        if let Some(size) = self.config.socket_recv_buffer_size {
            metadata_socket.set_recv_buffer_size(size)
                .map_err(|e| TransferError::NetworkError(format!("Failed to set SO_RCVBUF: {}", e)))?;
        }
        metadata_socket.set_nodelay(true)
            .map_err(|e| TransferError::NetworkError(format!("Failed to set TCP_NODELAY: {}", e)))?;

        let addr: SocketAddr = format!("0.0.0.0:{}", self.base_port)
            .parse()
            .map_err(|e| TransferError::NetworkError(format!("Invalid address: {}", e)))?;
        metadata_socket.bind(&addr.into())
            .map_err(|e| TransferError::NetworkError(format!("Failed to bind: {}", e)))?;
        metadata_socket.listen(128)
            .map_err(|e| TransferError::NetworkError(format!("Failed to listen: {}", e)))?;
        metadata_socket.set_nonblocking(true)
            .map_err(|e| TransferError::NetworkError(format!("Failed to set nonblocking: {}", e)))?;
        let std_listener = std::net::TcpListener::from(metadata_socket);
        let listener = TcpListener::from_std(std_listener)
            .map_err(|e| TransferError::NetworkError(format!("Failed to convert to tokio listener: {}", e)))?;
        let base_port = listener.local_addr()
            .map_err(|e| TransferError::NetworkError(format!("local_addr: {}", e)))?
            .port();
        if let Some(tx) = port_tx {
            let _ = tx.send(base_port);
        }
        eprintln!("Shift server listening on port {}", base_port);
        eprintln!("Output directory: {}", self.output_dir.display());
        #[cfg(feature = "tls")]
        if self.tls_acceptor.is_some() {
            eprintln!("TLS: mutual TLS enabled (cert dir from config)");
        }

        #[cfg(feature = "tls")]
        let tls_acceptor = self.tls_acceptor.clone();
        let completed_hashes = self.completed_hashes.clone();
        let persist_tx = self.persist_tx.clone();
        let output_dir = self.output_dir.clone();
        let config = self.config.clone();

        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    tracing::debug!(remote = %addr, "New metadata connection accepted");
                    let output_dir = output_dir.clone();
                    let config = config.clone();
                    let completed_hashes = completed_hashes.clone();
                    let persist_tx = persist_tx.clone();
                    #[cfg(feature = "tls")]
                    let tls_acceptor = tls_acceptor.clone();
                    tokio::spawn(async move {
                        #[cfg(feature = "tls")]
                        let result = async {
                            let stream = if let Some(ref acceptor) = tls_acceptor {
                                let tls_stream = acceptor.accept(stream).await.map_err(|e| TransferError::NetworkError(format!("TLS accept: {}", e)))?;
                                MetaStream::Tls(tls_stream)
                            } else {
                                MetaStream::Plain(stream)
                            };
                            Self::handle_connection(stream, addr, output_dir, base_port, config, completed_hashes, persist_tx, tls_acceptor).await
                        }.await;
                        #[cfg(feature = "tls")]
                        if let Err(e) = result {
                            error!(error = %e, "Connection handling failed");
                        }
                        #[cfg(not(feature = "tls"))]
                        let result = Self::handle_connection(stream, addr, output_dir, base_port, config, completed_hashes, persist_tx).await;
                        #[cfg(not(feature = "tls"))]
                        if let Err(e) = result {
                            error!(error = %e, "Connection handling failed");
                        }
                    });
                }
                Err(e) => {
                    error!(error = %e, "Failed to accept connection");
                }
            }
        }
    }

    /// Run one transfer then return. Binds to `0.0.0.0:0` (port 0); sends the assigned port on `port_tx` so the client can connect. Use for tests and oneshot usage.
    pub async fn run_oneshot(&self, port_tx: oneshot::Sender<u16>) -> Result<(), TransferError> {
        let metadata_socket = Socket::new(Domain::IPV4, Type::STREAM, None)
            .map_err(|e| TransferError::NetworkError(format!("Failed to create socket: {}", e)))?;
        if let Some(size) = self.config.socket_send_buffer_size {
            metadata_socket.set_send_buffer_size(size)
                .map_err(|e| TransferError::NetworkError(format!("Failed to set SO_SNDBUF: {}", e)))?;
        }
        if let Some(size) = self.config.socket_recv_buffer_size {
            metadata_socket.set_recv_buffer_size(size)
                .map_err(|e| TransferError::NetworkError(format!("Failed to set SO_RCVBUF: {}", e)))?;
        }
        metadata_socket.set_nodelay(true)
            .map_err(|e| TransferError::NetworkError(format!("Failed to set TCP_NODELAY: {}", e)))?;
        let addr: SocketAddr = "0.0.0.0:0".parse()
            .map_err(|e| TransferError::NetworkError(format!("Invalid address: {}", e)))?;
        metadata_socket.bind(&addr.into())
            .map_err(|e| TransferError::NetworkError(format!("Failed to bind: {}", e)))?;
        metadata_socket.listen(1)
            .map_err(|e| TransferError::NetworkError(format!("Failed to listen: {}", e)))?;
        metadata_socket.set_nonblocking(true)
            .map_err(|e| TransferError::NetworkError(format!("Failed to set nonblocking: {}", e)))?;
        let std_listener = std::net::TcpListener::from(metadata_socket);
        let listener = TcpListener::from_std(std_listener)
            .map_err(|e| TransferError::NetworkError(format!("Failed to convert to tokio listener: {}", e)))?;
        let base_port = listener.local_addr()
            .map_err(|e| TransferError::NetworkError(format!("local_addr: {}", e)))?
            .port();
        let _ = port_tx.send(base_port);

        let (stream, addr) = listener.accept().await
            .map_err(|e| TransferError::NetworkError(format!("Accept failed: {}", e)))?;
        tracing::debug!(remote = %addr, "Oneshot: accepted metadata connection");
        #[cfg(feature = "tls")]
        let result = {
            let tls_acceptor = self.tls_acceptor.clone();
            let stream = if let Some(ref acceptor) = tls_acceptor {
                let tls_stream = acceptor.accept(stream).await.map_err(|e| TransferError::NetworkError(format!("TLS accept: {}", e)))?;
                MetaStream::Tls(tls_stream)
            } else {
                MetaStream::Plain(stream)
            };
            Self::handle_connection(stream, addr, self.output_dir.clone(), base_port, self.config.clone(), self.completed_hashes.clone(), self.persist_tx.clone(), tls_acceptor).await
        };
        #[cfg(not(feature = "tls"))]
        let result = Self::handle_connection(stream, addr, self.output_dir.clone(), base_port, self.config.clone(), self.completed_hashes.clone(), self.persist_tx.clone()).await;
        result
    }

    async fn handle_connection<S>(
        mut stream: S,
        _addr: SocketAddr,
        output_dir: PathBuf,
        base_port: u16,
        config: TransferConfig,
        completed_hashes: Arc<Mutex<HashMap<PathBuf, [u8; BLAKE3_LEN]>>>,
        persist_tx: Option<std_mpsc::Sender<(PathBuf, [u8; BLAKE3_LEN])>>,
        #[cfg(feature = "tls")] tls_acceptor: Option<TlsAcceptor>,
    ) -> Result<(), TransferError>
    where
        S: AsyncReadExt + AsyncWriteExt + Unpin + Send,
    {
        // Capability handshake: read client caps, negotiate, send negotiated, apply to config.
        let mut cap_buf = [0u8; CAPABILITIES_WIRE_LEN];
        stream.read_exact(&mut cap_buf).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to read client capabilities: {}", e)))?;
        let client_caps = Capabilities::from_bytes(&cap_buf)
            .ok_or_else(|| TransferError::ProtocolError("Invalid client capabilities".to_string()))?;
        let server_caps = capabilities_from_config(&config);
        let negotiated = Capabilities::negotiate(client_caps, server_caps);
        stream.write_all(&negotiated.to_bytes()).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to send negotiated capabilities: {}", e)))?;
        let config = apply_capabilities_to_config(&config, negotiated);

        // After handshake: read 1 byte. If PING (RTT only) echo PONG and read 1 byte again. Then either CHECK_HASH (0x0A) or metadata (filename_len 8 + filename + file_size 8 + max_streams 8).
        let mut first_byte = [0u8; 1];
        if (negotiated.flags & cap_flags::RTT_PROBE) != 0 {
            stream.read_exact(&mut first_byte).await
                .map_err(|e| TransferError::NetworkError(format!("Failed to read after handshake: {}", e)))?;
            if first_byte[0] == msg::PING {
                let mut ts_buf = [0u8; 8];
                stream.read_exact(&mut ts_buf).await
                    .map_err(|e| TransferError::NetworkError(format!("Failed to read PING timestamp: {}", e)))?;
                let mut pong = [0u8; crate::base::PING_PONG_WIRE_LEN];
                pong[0] = msg::PONG;
                pong[1..9].copy_from_slice(&ts_buf);
                stream.write_all(&pong).await
                    .map_err(|e| TransferError::NetworkError(format!("Failed to send Pong: {}", e)))?;
                stream.read_exact(&mut first_byte).await
                    .map_err(|e| TransferError::NetworkError(format!("Failed to read after PONG: {}", e)))?;
            }
        } else {
            stream.read_exact(&mut first_byte).await
                .map_err(|e| TransferError::NetworkError(format!("Failed to read filename length: {}", e)))?;
        }

        let (filename, file_size, num_streams) = if first_byte[0] == msg::CHECK_HASH {
            // --update skip check: read filename_len (8) + filename + hash (32)
            let mut len_buf = [0u8; 8];
            stream.read_exact(&mut len_buf).await
                .map_err(|e| TransferError::NetworkError(format!("Failed to read CHECK_HASH filename len: {}", e)))?;
            let name_len = u64::from_le_bytes(len_buf) as usize;
            let mut name_buf = vec![0u8; name_len];
            stream.read_exact(&mut name_buf).await
                .map_err(|e| TransferError::NetworkError(format!("Failed to read CHECK_HASH filename: {}", e)))?;
            let check_filename = String::from_utf8(name_buf)
                .map_err(|e| TransferError::ProtocolError(format!("Invalid CHECK_HASH filename: {}", e)))?;
            let mut hash_buf = [0u8; BLAKE3_LEN];
            stream.read_exact(&mut hash_buf).await
                .map_err(|e| TransferError::NetworkError(format!("Failed to read CHECK_HASH hash: {}", e)))?;
            let path = output_dir.join(&check_filename);
            let cached_matches = completed_hashes.lock().unwrap().get(&path).map(|h| *h == hash_buf).unwrap_or(false);
            let have = if cached_matches {
                if path.exists() {
                    match std::fs::metadata(&path) {
                        Ok(meta) if meta.len() > 0 => {
                            match hash_file(&path, meta.len()) {
                                Ok(disk_hash) if disk_hash == hash_buf => true,
                                _ => {
                                    completed_hashes.lock().unwrap().remove(&path);
                                    false
                                }
                            }
                        }
                        _ => false,
                    }
                } else {
                    completed_hashes.lock().unwrap().remove(&path);
                    false
                }
            } else {
                false
            };
            if have {
                stream.write_all(&[msg::HAVE_HASH]).await
                    .map_err(|e| TransferError::NetworkError(format!("Failed to send HAVE_HASH: {}", e)))?;
                eprintln!("Skipped (unchanged): {}", check_filename);
                return Ok(());
            }
            stream.write_all(&[msg::NEED_FILE]).await
                .map_err(|e| TransferError::NetworkError(format!("Failed to send NEED_FILE: {}", e)))?;
            // Read full metadata: filename_len (8) + filename + file_size (8) + max_streams (8)
            stream.read_exact(&mut len_buf).await
                .map_err(|e| TransferError::NetworkError(format!("Failed to read metadata filename len: {}", e)))?;
            let name_len = u64::from_le_bytes(len_buf) as usize;
            let mut name_buf = vec![0u8; name_len];
            stream.read_exact(&mut name_buf).await
                .map_err(|e| TransferError::NetworkError(format!("Failed to read metadata filename: {}", e)))?;
            let filename = String::from_utf8(name_buf)
                .map_err(|e| TransferError::ProtocolError(format!("Invalid filename: {}", e)))?;
            let mut file_size_buf = [0u8; 8];
            stream.read_exact(&mut file_size_buf).await
                .map_err(|e| TransferError::NetworkError(format!("Failed to read file size: {}", e)))?;
            let file_size = u64::from_le_bytes(file_size_buf);
            let mut num_streams_buf = [0u8; 8];
            stream.read_exact(&mut num_streams_buf).await
                .map_err(|e| TransferError::NetworkError(format!("Failed to read num streams: {}", e)))?;
            let num_streams = u64::from_le_bytes(num_streams_buf) as usize;
            (filename, file_size, num_streams)
        } else {
            let mut filename_len_buf = [0u8; 8];
            filename_len_buf[0] = first_byte[0];
            stream.read_exact(&mut filename_len_buf[1..8]).await
                .map_err(|e| TransferError::NetworkError(format!("Failed to read filename length: {}", e)))?;
            let filename_len = u64::from_le_bytes(filename_len_buf) as usize;
            let mut filename_buf = vec![0u8; filename_len];
            stream.read_exact(&mut filename_buf).await
                .map_err(|e| TransferError::NetworkError(format!("Failed to read filename: {}", e)))?;
            let filename = String::from_utf8(filename_buf)
                .map_err(|e| TransferError::ProtocolError(format!("Invalid filename: {}", e)))?;
            let mut file_size_buf = [0u8; 8];
            stream.read_exact(&mut file_size_buf).await
                .map_err(|e| TransferError::NetworkError(format!("Failed to read file size: {}", e)))?;
            let file_size = u64::from_le_bytes(file_size_buf);
            let mut num_streams_buf = [0u8; 8];
            stream.read_exact(&mut num_streams_buf).await
                .map_err(|e| TransferError::NetworkError(format!("Failed to read num streams: {}", e)))?;
            let num_streams = u64::from_le_bytes(num_streams_buf) as usize;
            (filename, file_size, num_streams)
        };

        // Minimal logging - just the filename
        eprintln!("Receiving: {}", filename);

        // Ensure output directory exists
        std::fs::create_dir_all(&output_dir)?;

        // Create data listeners BEFORE sending ACK so they're ready when client connects
        let output_path = output_dir.join(&filename);
        let file = std::fs::File::create(&output_path)?;
        file.set_len(file_size)?;
        let file = Arc::new(file);

        let num_ranges = crate::base::transfer_num_ranges(num_streams);
        let ranges = crate::base::split_file_ranges(file_size, num_ranges);
        let mut data_handles = Vec::new();

        let (range_hash_tx, mut range_hash_rx) = mpsc::channel::<(FileRange, [u8; BLAKE3_LEN])>(64);

        #[cfg(feature = "tls")]
        let tls_acceptor = tls_acceptor.clone();
        for (thread_id, _range) in ranges.into_iter().enumerate() {
            let file = Arc::clone(&file);
            let config = config.clone();
            let data_port = base_port + 1 + thread_id as u16;
            let range_hash_tx = range_hash_tx.clone();
            #[cfg(feature = "tls")]
            let tls_acceptor = tls_acceptor.clone();

            let handle = tokio::spawn(async move {
                // Create and configure listening socket
                let socket = Socket::new(Domain::IPV4, Type::STREAM, None)
                    .map_err(|e| TransferError::NetworkError(format!("Failed to create socket: {}", e)))?;
                configure_tcp_socket(
                    &socket,
                    config.socket_send_buffer_size,
                    config.socket_recv_buffer_size,
                )?;

                let addr: SocketAddr = format!("0.0.0.0:{}", data_port)
                    .parse()
                    .map_err(|e| TransferError::NetworkError(format!("Invalid address: {}", e)))?;
                
                socket.bind(&addr.into())
                    .map_err(|e| TransferError::NetworkError(format!("Failed to bind: {}", e)))?;
                socket.listen(1)
                    .map_err(|e| TransferError::NetworkError(format!("Failed to listen: {}", e)))?;

                socket.set_nonblocking(true)
                    .map_err(|e| TransferError::NetworkError(format!("Failed to set nonblocking: {}", e)))?;
                
                let std_listener = std::net::TcpListener::from(socket);
                let listener = TcpListener::from_std(std_listener)
                    .map_err(|e| TransferError::NetworkError(format!("Failed to convert to tokio listener: {}", e)))?;

                // Accept connection
                let (data_stream, _) = listener.accept().await
                    .map_err(|e| TransferError::NetworkError(format!("Accept failed: {}", e)))?;

                #[cfg(feature = "tls")]
                let data_stream = {
                    if let Some(acceptor) = tls_acceptor {
                        let tls_stream = acceptor
                            .accept(data_stream)
                            .await
                            .map_err(|e| TransferError::NetworkError(format!("TLS accept data: {}", e)))?;
                        MetaStream::Tls(tls_stream)
                    } else {
                        MetaStream::Plain(data_stream)
                    }
                };

                receive_range_tcp(thread_id, file, data_stream, config, None, Some(range_hash_tx)).await
            });

            data_handles.push(handle);
        }

        stream.write_all(&[msg::READY]).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to send initial ack: {}", e)))?;

        // Optional per-range verification (0x06) before data streams. Client sends 0x00 or 0x06.
        let mut range_verify_byte = [0u8; 1];
        stream.read_exact(&mut range_verify_byte).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to read range verify byte: {}", e)))?;
        if range_verify_byte[0] == msg::RANGE_HASHES {
            let mut num_ranges_buf = [0u8; 8];
            stream.read_exact(&mut num_ranges_buf).await
                .map_err(|e| TransferError::NetworkError(format!("Failed to read num_ranges: {}", e)))?;
            let num_ranges = u64::from_le_bytes(num_ranges_buf) as usize;
            let mut failed: Vec<(u64, u64)> = Vec::new();
            for _ in 0..num_ranges {
                let mut start_buf = [0u8; 8];
                let mut end_buf = [0u8; 8];
                let mut expected_hash = [0u8; BLAKE3_LEN];
                stream.read_exact(&mut start_buf).await
                    .map_err(|e| TransferError::NetworkError(format!("Failed to read range start: {}", e)))?;
                stream.read_exact(&mut end_buf).await
                    .map_err(|e| TransferError::NetworkError(format!("Failed to read range end: {}", e)))?;
                stream.read_exact(&mut expected_hash).await
                    .map_err(|e| TransferError::NetworkError(format!("Failed to read range hash: {}", e)))?;
                let start = u64::from_le_bytes(start_buf);
                let end = u64::from_le_bytes(end_buf);
                match hash_file_range_path(&output_path, start, end) {
                    Ok(actual) if actual == expected_hash => {}
                    _ => failed.push((start, end)),
                }
            }
            stream.write_all(&[msg::RANGE_VERIFY_RESULT]).await
                .map_err(|e| TransferError::NetworkError(format!("Failed to send range verify result: {}", e)))?;
            stream.write_all(&(failed.len() as u64).to_le_bytes()).await
                .map_err(|e| TransferError::NetworkError(format!("Failed to send num_failed: {}", e)))?;
            for (start, end) in &failed {
                stream.write_all(&start.to_le_bytes()).await
                    .map_err(|e| TransferError::NetworkError(format!("Failed to send failed start: {}", e)))?;
                stream.write_all(&end.to_le_bytes()).await
                    .map_err(|e| TransferError::NetworkError(format!("Failed to send failed end: {}", e)))?;
            }
        }
        // else 0x00: no range verify, proceed to data listeners

        // Wait for all data connections to complete. Defer returning error so we can do hash
        // exchange and send HASH_OK/HASH_MISMATCH before closing (avoids client "early eof").
        let mut total_received = 0u64;
        let mut data_error: Option<TransferError> = None;
        for (thread_id, handle) in data_handles.into_iter().enumerate() {
            match handle.await {
                Ok(Ok(bytes)) => {
                    total_received += bytes;
                    tracing::debug!(
                        thread_id,
                        bytes,
                        "Data connection completed"
                    );
                }
                Ok(Err(e)) => {
                    if data_error.is_none() {
                        data_error = Some(e);
                    }
                }
                Err(e) => {
                    if data_error.is_none() {
                        data_error = Some(TransferError::NetworkError(format!("Data connection panicked: {:?}", e)));
                    }
                }
            }
        }

        // Sync file to ensure all data is written to disk
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            use libc;
            let fd = file.as_raw_fd();
            unsafe {
                libc::fsync(fd);
            }
        }

        #[cfg(not(unix))]
        {
            let file_mut = std::fs::File::try_clone(&*file)?;
            file_mut.sync_all()?;
        }

        let mut hash_type_buf = [0u8; 1];
        stream.read_exact(&mut hash_type_buf).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to read hash message type: {}", e)))?;
        if hash_type_buf[0] != msg::HASH {
            return Err(TransferError::ProtocolError(format!(
                "Expected hash message (0x03), got 0x{:02x}",
                hash_type_buf[0]
            )));
        }
        let mut expected_hash = [0u8; BLAKE3_LEN];
        stream.read_exact(&mut expected_hash).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to read file hash: {}", e)))?;

        drop(range_hash_tx);
        let mut received_ranges: Vec<(FileRange, [u8; BLAKE3_LEN])> = Vec::new();
        while let Some(rh) = range_hash_rx.recv().await {
            received_ranges.push(rh);
        }
        let actual_hash = hash_file(&output_path, file_size)?;
        if actual_hash != expected_hash {
            // Tell client to keep checkpoint for retry (0x05), then return error.
            let _ = stream.write_all(&[msg::HASH_MISMATCH]).await;
            return Err(TransferError::IntegrityCheckFailed(format!(
                "BLAKE3 mismatch for {} (transfer may be corrupted)",
                filename
            )));
        }

        stream.write_all(&[msg::HASH_OK]).await
            .map_err(|e| TransferError::NetworkError(format!("Failed to send hash OK: {}", e)))?;
        stream.flush().await
            .map_err(|e| TransferError::NetworkError(format!("Failed to flush hash OK: {}", e)))?;
        // Drain read until client closes (close handshake: client reads HASH_OK then drops connection, we see EOF).
        let mut buf = [0u8; 256];
        loop {
            match stream.read(&mut buf).await {
                Ok(0) => break,
                Ok(_) => {}
                Err(_) => break,
            }
        }

        if data_error.is_none() {
            completed_hashes.lock().unwrap().insert(output_path.clone(), expected_hash);
            if let Some(ref tx) = persist_tx {
                let _ = tx.send((output_path.clone(), expected_hash));
            }
            eprintln!("Received: {} ({} bytes)", filename, total_received);
        }

        if let Some(e) = data_error {
            return Err(e);
        }
        Ok(())
    }
}

