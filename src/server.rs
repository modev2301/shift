//! Server-side file transfer functionality.
//!
//! This module contains the server implementation for receiving files from clients.
//! It handles incoming connections, manages transfer sessions, and writes received
//! chunks to disk. The server minimizes ACKs and relies on TCP flow control
//! for optimal performance.

use crate::client::{Message, TransferSession};
use crate::compression::FileChunk;
use crate::compression::ResumeInfo;
use crate::config::ServerConfig;
use crate::error::TransferError;
use crate::utils::ChunkPool;
use indicatif::MultiProgress;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, Mutex};
use tracing::{debug, error, info, warn};

pub struct TransferServer {
    config: Arc<ServerConfig>,
    sessions: Arc<Mutex<HashMap<String, Arc<Mutex<TransferSession>>>>>,
    multi_progress: MultiProgress,
}

impl TransferServer {
    pub fn new(config: Arc<ServerConfig>) -> Self {
        Self {
            config,
            sessions: Arc::new(Mutex::new(HashMap::new())),
            multi_progress: MultiProgress::new(),
        }
    }

    /// Starts the transfer server and begins accepting connections.
    ///
    /// This method binds to the configured address and port, creates the output
    /// directory if needed, and spawns a task for each incoming connection.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Binding to the address/port fails
    /// - Creating the output directory fails
    ///
    /// # Returns
    ///
    /// This method never returns under normal operation. It runs an infinite loop
    /// accepting connections until the process is terminated.
    pub async fn run(&self) -> Result<(), TransferError> {
        let addr = format!("{}:{}", self.config.address, self.config.port);
        info!(address = %addr, "Transfer server starting");

        let listener = TcpListener::bind(&addr).await.map_err(|e| {
            TransferError::NetworkError(format!("Failed to bind to {}: {}", addr, e))
        })?;

        info!(address = %addr, "Server listening for connections");
        info!(output_directory = %self.config.output_directory, "Output directory configured");

        // Ensure output directory exists before accepting connections.
        // This prevents errors when clients try to transfer files.
        tokio::fs::create_dir_all(&self.config.output_directory)
            .await
            .map_err(|e| TransferError::Io(e))?;

        loop {
            match listener.accept().await {
                Ok((stream, peer_addr)) => {
                    info!(peer_addr = %peer_addr, "New connection accepted");

                    let config = Arc::clone(&self.config);
                    let sessions = Arc::clone(&self.sessions);
                    let multi_progress = self.multi_progress.clone();

                    // Spawn a task to handle this connection independently.
                    // This allows the server to accept multiple concurrent transfers.
                    tokio::spawn(async move {
                        if let Err(e) =
                            Self::handle_client(stream, config, sessions, multi_progress).await
                        {
                            error!(error = %e, "Error handling client connection");
                        }
                    });
                }
                Err(e) => {
                    error!(error = %e, "Failed to accept connection");
                }
            }
        }
    }

    /// Handles a single client connection for the duration of a file transfer.
    ///
    /// This function processes the handshake, creates or retrieves a transfer session,
    /// and then receives file chunks until the transfer is complete. It minimizes ACKs
    /// and relies on TCP flow control for optimal performance.
    ///
    /// # Arguments
    ///
    /// * `stream` - The TCP stream for this client connection
    /// * `config` - Server configuration
    /// * `sessions` - Shared map of active transfer sessions
    /// * `_multi_progress` - Progress bar manager (currently unused)
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - TCP options cannot be set
    /// - Handshake is invalid or missing
    /// - Authentication fails
    /// - Connection closes prematurely with incomplete transfer
    async fn handle_client(
        mut stream: TcpStream,
        config: Arc<ServerConfig>,
        sessions: Arc<Mutex<HashMap<String, Arc<Mutex<TransferSession>>>>>,
        _multi_progress: MultiProgress,
    ) -> Result<(), TransferError> {
        // Enable TCP_NODELAY to reduce latency by disabling Nagle's algorithm.
        // This is important for high-throughput transfers where we want immediate
        // packet transmission rather than batching small packets.
        stream.set_nodelay(true).map_err(|e| {
            TransferError::NetworkError(format!("Failed to set TCP_NODELAY: {}", e))
        })?;

        // Read and validate the handshake message. The handshake establishes the
        // transfer session and provides metadata about the file being transferred.
        let handshake = match Message::read_from_stream(&mut stream).await? {
            Some(Message::Handshake {
                session_id,
                filename,
                file_size,
                chunk_size,
                parallel_streams: _,
                compression_enabled: _,
                auth_token,
                resume_info,
            }) => {
                // Validate authentication token before proceeding.
                // In production, this should use proper cryptographic verification.
                const EXPECTED_TOKEN: &str = "shift_default_token";
                if auth_token != EXPECTED_TOKEN {
                    let error_msg = Message::Error {
                        session_id: session_id.clone(),
                        error_message: "Invalid auth token".to_string(),
                    };
                    error_msg.write_to_stream(&mut stream).await?;
                    return Err(TransferError::NetworkError(
                        "Invalid auth token".to_string(),
                    ));
                }

                info!(
                    session_id = %session_id,
                    filename = %filename,
                    file_size = file_size,
                    chunk_size = chunk_size,
                    "Handshake received"
                );

                (session_id, filename, file_size, chunk_size, resume_info)
            }
            Some(msg) => {
                error!(received_message = ?msg, "Expected handshake message");
                return Err(TransferError::NetworkError(
                    "Expected handshake message".to_string(),
                ));
            }
            None => {
                return Err(TransferError::NetworkError(
                    "Connection closed before handshake".to_string(),
                ));
            }
        };

        let (session_id, filename, file_size, chunk_size, resume_info) = handshake;

        // Retrieve or create the transfer session for this connection.
        // Sessions are shared across connections to support resumption scenarios
        // where a client might reconnect to continue a previous transfer.
        let session = {
            let mut sessions = sessions.lock().await;

            if let Some(existing) = sessions.get(&session_id) {
                Arc::clone(existing)
            } else {
                // Create a new transfer session for this file transfer.
                // The session manages chunk reception, file writing, and completion tracking.
                let output_dir = PathBuf::from(&config.output_directory);

                let (completion_tx, _completion_rx) = oneshot::channel();
                let (ack_tx, _ack_rx) = mpsc::channel(1000);

                let resume = resume_info.map(ResumeInfo::from);

                // Create a chunk pool for efficient memory management.
                // The pool reuses buffers to reduce allocation overhead during transfers.
                let chunk_pool = Arc::new(ChunkPool::new(
                    config.buffer_size.unwrap_or(64 * 1024),
                    2000,
                ));

                let transfer_session = TransferSession::new(
                    session_id.clone(),
                    filename.clone(),
                    file_size,
                    config.enable_progress_bar,
                    chunk_size,
                    completion_tx,
                    config.parallel_streams.unwrap_or(4),
                    resume,
                    ack_tx,
                    chunk_pool,
                    output_dir,
                );

                let session = Arc::new(Mutex::new(transfer_session));
                sessions.insert(session_id.clone(), Arc::clone(&session));
                session
            }
        };

        // Split the stream into separate reader and writer halves.
        // This allows concurrent reading and writing without explicit locking.
        let (mut reader, mut writer) = stream.split();

        // Minimize ACKs by relying on TCP flow control.
        // TCP's built-in flow control automatically handles backpressure, eliminating
        // thousands of round-trips that would occur with per-chunk acknowledgments.
        // We only send ACKs for errors or transfer completion.

        // Main transfer loop: receive chunks until transfer is complete.
        // We track whether EndOfStream has been received to handle cases where
        // chunks might arrive out of order or the connection closes early.
        let mut end_of_stream_received = false;
        let total_chunks = (file_size + chunk_size as u64 - 1) / chunk_size as u64;

        loop {
            // After EndOfStream is received, use a shorter timeout to periodically
            // check if all chunks have arrived. This handles cases where chunks
            // are still in-flight when EndOfStream is sent.
            let msg = if end_of_stream_received {
                const POST_EOS_TIMEOUT_SECS: u64 = 5; // Increased from 2 to 5 seconds
                match tokio::time::timeout(
                    tokio::time::Duration::from_secs(POST_EOS_TIMEOUT_SECS),
                    Message::read_from_stream(&mut reader),
                )
                .await
                {
                    Ok(Ok(msg)) => msg,
                    Ok(Err(e)) => {
                        // Connection error - check if all chunks received before failing
                        let received_count = {
                            let session_guard = session.lock().await;
                            session_guard.received_chunk_ids.len() as u64
                        };
                        if received_count >= total_chunks {
                            // All chunks received despite error - send ACK if possible
                            let ack = Message::ChunkAck {
                                session_id: session_id.clone(),
                                chunk_id: total_chunks - 1,
                                success: true,
                                error_message: None,
                            };
                            let _ = ack.write_to_stream(&mut writer).await;
                            info!(
                                filename = %filename,
                                chunks_received = received_count,
                                "Transfer completed despite read error"
                            );
                            break;
                        }
                        return Err(e);
                    }
                    Err(_) => {
                        // Timeout occurred - check if all chunks have been received.
                        // This handles the case where EndOfStream was sent but some
                        // chunks are still in the network buffer.
                        let received_count = {
                            let session_guard = session.lock().await;
                            session_guard.received_chunk_ids.len() as u64
                        };

                        if received_count >= total_chunks {
                            // All chunks received, send completion ACK and finish.
                            let ack = Message::ChunkAck {
                                session_id: session_id.clone(),
                                chunk_id: total_chunks - 1,
                                success: true,
                                error_message: None,
                            };
                            if let Err(e) = ack.write_to_stream(&mut writer).await {
                                error!(
                                    error = %e,
                                    "Failed to send completion ACK after timeout"
                                );
                            } else {
                                info!(
                                    filename = %filename,
                                    chunks_received = received_count,
                                    total_chunks = total_chunks,
                                    "Transfer completed after EndOfStream timeout"
                                );
                            }
                            break;
                        } else {
                            // Still missing chunks, continue waiting for them to arrive.
                            let missing = total_chunks - received_count;
                            debug!(
                                chunks_received = received_count,
                                total_chunks = total_chunks,
                                missing_chunks = missing,
                                "Still waiting for {} chunks after timeout",
                                missing
                            );
                            continue;
                        }
                    }
                }
            } else {
                // Normal read without timeout. We only use timeouts after EndOfStream
                // to avoid unnecessary delays during active transfer.
                match Message::read_from_stream(&mut reader).await {
                    Ok(msg) => msg,
                    Err(e) => {
                        // Connection error occurred. Before failing, verify that
                        // all chunks were actually received. This handles cases where
                        // the connection closes but all data was successfully transferred.
                        let received_count = {
                            let session_guard = session.lock().await;
                            session_guard.received_chunk_ids.len() as u64
                        };

                        if received_count >= total_chunks {
                            // All chunks received despite connection error.
                            // Send completion ACK and finish successfully.
                            let ack = Message::ChunkAck {
                                session_id: session_id.clone(),
                                chunk_id: total_chunks - 1,
                                success: true,
                                error_message: None,
                            };
                            let _ = ack.write_to_stream(&mut writer).await;
                            info!(
                                filename = %filename,
                                chunks_received = received_count,
                                "Transfer completed despite connection error"
                            );
                            break;
                        } else {
                            // Not all chunks received - this is a real error.
                            return Err(e);
                        }
                    }
                }
            };

            match msg {
                Some(Message::FileChunk {
                    chunk_id,
                    session_id: msg_session_id,
                    data,
                    is_last,
                    is_compressed,
                    checksum,
                    original_size,
                    sequence_number: _,
                    retry_count: _,
                }) => {
                    // Validate session ID to ensure this chunk belongs to the current transfer.
                    // This prevents chunks from one transfer being applied to another.
                    if msg_session_id != session_id {
                        warn!(
                            expected_session = %session_id,
                            received_session = %msg_session_id,
                            chunk_id = chunk_id,
                            "Session ID mismatch, ignoring chunk"
                        );
                        continue;
                    }

                    // Convert the message into a FileChunk for processing.
                    // The session will handle validation, decompression, and writing.
                    let chunk = FileChunk {
                        chunk_id,
                        session_id: session_id.clone(),
                        data,
                        is_last,
                        is_compressed,
                        checksum,
                        original_size,
                        sequence_number: 0,
                        retry_count: 0,
                    };

                    // Process the chunk: validate, decompress if needed, and write to disk.
                    // We don't send ACKs for every chunk - TCP's flow control handles
                    // backpressure automatically. This eliminates thousands of round-trips
                    // and dramatically improves throughput.
                    let mut session_guard = session.lock().await;
                    match session_guard.add_chunk(chunk).await {
                        Ok(completed) => {
                            if completed {
                                // Transfer is complete - send final ACK to confirm.
                                // This is the only ACK we send for successful transfers.
                                let ack = Message::ChunkAck {
                                    session_id: session_id.clone(),
                                    chunk_id,
                                    success: true,
                                    error_message: None,
                                };
                                let _ = ack.write_to_stream(&mut writer).await;
                                info!(
                                    filename = %filename,
                                    chunk_id = chunk_id,
                                    "Transfer completed successfully"
                                );
                                break;
                            }
                            // Chunk processed successfully, but more chunks expected.
                            // No ACK sent - TCP handles flow control.
                        }
                        Err(e) => {
                            error!(
                                chunk_id = chunk_id,
                                error = %e,
                                "Failed to process chunk"
                            );

                            // Send error ACK immediately. Unlike successful chunks,
                            // errors require immediate feedback so the client can
                            // retry or abort the transfer.
                            let ack = Message::ChunkAck {
                                session_id: session_id.clone(),
                                chunk_id,
                                success: false,
                                error_message: Some(e.to_string()),
                            };

                            let _ = ack.write_to_stream(&mut writer).await;
                        }
                    }
                }
                Some(Message::EndOfStream {
                    session_id: msg_session_id,
                    ..
                }) => {
                    if msg_session_id == session_id {
                        info!(
                            session_id = %session_id,
                            "End of stream marker received"
                        );
                        end_of_stream_received = true;

                        // Verify that all chunks have been received before completing.
                        // EndOfStream is sent after all chunks are queued, but chunks
                        // may still be in-flight, so we need to verify completion.
                        let received_count = {
                            let session_guard = session.lock().await;
                            session_guard.received_chunk_ids.len() as u64
                        };

                        if received_count >= total_chunks {
                            // All chunks received - send completion ACK and finish.
                            let ack = Message::ChunkAck {
                                session_id: session_id.clone(),
                                chunk_id: total_chunks - 1,
                                success: true,
                                error_message: None,
                            };
                            if let Err(e) = ack.write_to_stream(&mut writer).await {
                                error!(
                                    error = %e,
                                    "Failed to send completion ACK"
                                );
                            } else {
                                info!(
                                    filename = %filename,
                                    chunks_received = received_count,
                                    total_chunks = total_chunks,
                                    "Transfer completed, ACK sent"
                                );
                            }
                            break;
                        } else {
                            // Not all chunks received yet. Give a brief moment for in-flight chunks
                            // to arrive, then check again. This handles the case where EndOfStream
                            // arrives slightly before the last chunks due to network reordering.
                            let missing = total_chunks - received_count;
                            info!(
                                chunks_received = received_count,
                                total_chunks = total_chunks,
                                missing_chunks = missing,
                                "EndOfStream received, waiting for {} remaining chunks",
                                missing
                            );
                            // Give a short grace period for in-flight chunks
                            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                            // Continue the loop - the timeout mechanism will check periodically
                        }
                    }
                }
                Some(Message::Error { error_message, .. }) => {
                    error!(
                        error_message = %error_message,
                        "Client reported error"
                    );
                    break;
                }
                Some(_) => {
                    warn!("Received unexpected message type");
                }
                None => {
                    // Connection closed (EOF). Check if transfer was actually complete.
                    // This handles cases where the client closes the connection after
                    // sending all data but before receiving the completion ACK.
                    let received_count = {
                        let session_guard = session.lock().await;
                        session_guard.received_chunk_ids.len() as u64
                    };

                    if received_count >= total_chunks {
                        info!(
                            filename = %filename,
                            chunks_received = received_count,
                            "Connection closed after successful transfer"
                        );
                        // Transfer is complete, but connection is already closed.
                        // Can't send ACK, but that's okay - the file is written correctly.
                        break;
                    } else if end_of_stream_received {
                        // EndOfStream was received, but connection closed before all chunks.
                        // Give a brief moment for any in-flight chunks to be processed,
                        // then verify final state.
                        const IN_FLIGHT_WAIT_MS: u64 = 500;
                        tokio::time::sleep(tokio::time::Duration::from_millis(IN_FLIGHT_WAIT_MS))
                            .await;

                        let final_count = {
                            let session_guard = session.lock().await;
                            session_guard.received_chunk_ids.len() as u64
                        };

                        if final_count >= total_chunks {
                            info!(
                                filename = %filename,
                                chunks_received = final_count,
                                "Transfer completed after connection close"
                            );
                            break;
                        } else {
                            error!(
                                chunks_received = final_count,
                                total_chunks = total_chunks,
                                "Connection closed prematurely with incomplete transfer"
                            );
                            return Err(TransferError::NetworkError(format!(
                                "Connection closed early: {}/{} chunks received",
                                final_count, total_chunks
                            )));
                        }
                    } else {
                        error!(
                            chunks_received = received_count,
                            total_chunks = total_chunks,
                            "Connection closed before EndOfStream with incomplete transfer"
                        );
                        return Err(TransferError::NetworkError(format!(
                            "Connection closed early: {}/{} chunks received",
                            received_count, total_chunks
                        )));
                    }
                }
            }
        }

        Ok(())
    }
}
