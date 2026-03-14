//! Integration tests: real protocol end-to-end over loopback.
//!
//! - `test_loopback_tcp_transfer_end_to_end`: in-process, oneshot server (port 0) + library client; fast, no subprocess.
//! - `test_loopback_via_cli`: full binary as server and client (subprocess); catches CLI/config wiring.
//! - `test_loopback_tcp_update_skip_unchanged`: in-process, tests --update skip when file unchanged.
//! - `test_loopback_detects_corruption`: in-process, corrupt received file then --update; verifies re-transfer (BLAKE3 / cache invalidation).

use shift::integrity::hash_file;
use shift::tcp_server::TcpServer;
use shift::tcp_transfer::send_file_over_transport;
use shift::transport::create_transport;
use shift::TransferConfig;
use std::net::SocketAddr;
use std::process::Command;
use std::time::Duration;
use tempfile::tempdir;

/// Pick a free port (for CLI test that starts a separate process). In-process tests use port 0 + oneshot to get the assigned port.
fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind to 0");
    listener.local_addr().expect("local_addr").port()
}

/// Poll until the server is accepting on `port` or `timeout` expires. Avoids fragile fixed sleeps on slow CI.
fn wait_for_port(port: u16, timeout: Duration) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if std::net::TcpStream::connect(format!("127.0.0.1:{}", port)).is_ok() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    false
}

fn transfer_config(port: u16) -> TransferConfig {
    TransferConfig {
        start_port: port,
        num_streams: 4,
        max_streams: 8,
        buffer_size: 256 * 1024,
        socket_send_buffer_size: Some(256 * 1024),
        socket_recv_buffer_size: Some(256 * 1024),
        enable_compression: false,
        enable_encryption: false,
        encryption_key: None,
        timeout_seconds: 30,
        tls_cert_dir: None,
        enable_fec: false,
        fec_block_size: 65536,
        fec_repair_packets: 4,
        #[cfg(feature = "fec")]
        fec_negotiated: false,
    }
}

/// In-process: oneshot server (port 0) + library client. Proper close handshake so client receives HASH_OK before server returns.
#[tokio::test]
async fn test_loopback_tcp_transfer_end_to_end() {
    let out_dir = tempdir().expect("temp dir");
    let out_path = out_dir.path().to_path_buf();
    let src_dir = tempdir().expect("temp dir for src");
    let src_path = src_dir.path().join("data.bin");
    let size = 64 * 1024u64;
    let content: Vec<u8> = (0..size as usize).map(|i| (i % 251) as u8).collect();
    std::fs::write(&src_path, &content).expect("write");
    let expected_hash = hash_file(&src_path, size).expect("hash source");

    let (port_tx, port_rx) = tokio::sync::oneshot::channel();
    let server = TcpServer::new(0, out_path.clone(), transfer_config(0));
    let server_handle = tokio::spawn(async move {
        let _ = server.run_oneshot(port_tx).await;
    });

    let port = port_rx.await.expect("server should send port");
    tokio::time::sleep(Duration::from_millis(100)).await;
    let config = transfer_config(port);
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().expect("parse addr");
    let transport = create_transport(&config, true).await;
    let result = send_file_over_transport(
        transport.as_ref(),
        &src_path,
        addr,
        config.clone(),
        None,
        false,
        None,
    )
    .await;

    let sent = result.expect("transfer should succeed");
    assert_eq!(sent, size, "bytes sent should match file size");

    let received_path = out_path.join("data.bin");
    assert!(received_path.exists(), "received file should exist");
    let received_meta = std::fs::metadata(&received_path).expect("metadata");
    assert_eq!(received_meta.len(), size, "received size should match");
    let received_hash = hash_file(&received_path, size).expect("hash received");
    assert_eq!(received_hash[..], expected_hash[..], "BLAKE3 of received file should match source");

    let _ = server_handle.await;
}

/// Full protocol test using the shift CLI: start server subprocess, send file, verify.
/// Requires the binary to be built (`cargo build` or `cargo build --bin shift`).
#[test]
fn test_loopback_via_cli() {
    let bin_path = std::env::var("CARGO_BIN_EXE_shift")
        .ok()
        .map(std::path::PathBuf::from)
        .or_else(|| {
            std::env::var("CARGO_MANIFEST_DIR").ok().map(|m| {
                std::path::PathBuf::from(m).join("target/debug/shift")
            })
        })
        .and_then(|p| p.canonicalize().ok());
    let bin_path = match bin_path {
        Some(p) => p,
        None => {
            eprintln!("Skipping test_loopback_via_cli: binary not found (run cargo build --bin shift)");
            return;
        }
    };

    let port = free_port();
    let dir = tempdir().expect("temp dir");
    let dir_path = dir.path();
    let output_dir = dir_path.join("received");
    std::fs::create_dir_all(&output_dir).expect("create received dir");

    let config_toml = format!(
        r#"[server]
address = "127.0.0.1"
port = {}
output_directory = "received"
max_clients = 10
enable_compression = false
timeout_seconds = 30

[client]
server_address = "127.0.0.1"
server_port = {}
enable_compression = false
timeout_seconds = 30

[security]
auth_token = "test"
max_connections_per_ip = 10

[performance]
simd_enabled = true
zero_copy_enabled = true
memory_pool_size = 2000
connection_pool_size = 100
compression_level = 1
metrics_enabled = true
"#,
        port, port
    );
    std::fs::write(dir_path.join("config.toml"), config_toml).expect("write config");

    let size = 32 * 1024u64;
    let content: Vec<u8> = (0..size as usize).map(|i| (i % 251) as u8).collect();
    let source_path = dir_path.join("data.bin");
    std::fs::write(&source_path, &content).expect("write data");
    let expected_hash = hash_file(&source_path, size).expect("hash");

    let mut server = Command::new(&bin_path)
        .args(["server"])
        .current_dir(dir_path)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn server");

    assert!(
        wait_for_port(port, Duration::from_secs(5)),
        "server did not start in time"
    );

    let dest = format!("127.0.0.1:{}:/", port);
    let status = Command::new(&bin_path)
        .args(["--tcp", "data.bin", &dest])
        .current_dir(dir_path)
        .status()
        .expect("run client");

    let _ = server.kill();
    let _ = server.wait();

    assert!(status.success(), "client should succeed");
    // Hash the source again after transfer to confirm it didn't change
    let post_hash = hash_file(&source_path, size).expect("hash source after transfer");
    assert_eq!(post_hash[..], expected_hash[..], "source file changed during transfer!");
    let received = output_dir.join("data.bin");
    assert!(received.exists(), "received file should exist");
    let meta = std::fs::metadata(&received).expect("metadata");
    assert_eq!(
        meta.len(),
        size,
        "expected size {}, received size {} — truncation or wrong file",
        size,
        meta.len()
    );
    let got_hash = hash_file(&received, size).expect("hash received");
    assert_eq!(
        got_hash[..],
        expected_hash[..],
        "BLAKE3 should match (size matched, so likely corruption or wrong path; received path: {})",
        received.display()
    );
}

#[tokio::test]
async fn test_loopback_tcp_update_skip_unchanged() {
    let out_dir = tempdir().expect("temp dir");
    let out_path = out_dir.path().to_path_buf();
    let src_dir = tempdir().expect("temp dir for src");
    let src_path = src_dir.path().join("small.bin");
    let size = 32 * 1024u64;
    let content: Vec<u8> = (0..size as usize).map(|i| (i % 197) as u8).collect();
    std::fs::write(&src_path, &content).expect("write");

    let (port_tx, port_rx) = tokio::sync::oneshot::channel();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server = TcpServer::new(0, out_path.clone(), transfer_config(0));
    let server_handle = tokio::spawn(async move {
        tokio::select! {
            _ = server.run_forever(Some(port_tx)) => {}
            _ = shutdown_rx => {}
        }
    });

    let port = port_rx.await.expect("server should send port");
    tokio::time::sleep(Duration::from_millis(100)).await;
    let config = transfer_config(port);
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().expect("parse addr");
    let transport = create_transport(&config, true).await;

    let sent1 = send_file_over_transport(
        transport.as_ref(),
        &src_path,
        addr,
        config.clone(),
        None,
        false,
        None,
    )
    .await
    .expect("first transfer");
    assert_eq!(sent1, size);

    let mut report = shift::TransferReport::default();
    let sent2 = send_file_over_transport(
        transport.as_ref(),
        &src_path,
        addr,
        config.clone(),
        Some(&mut report),
        true,
        None,
    )
    .await
    .expect("second transfer (skip)");
    assert_eq!(sent2, 0, "skipped transfer should return 0 bytes");
    assert!(matches!(report.status, shift::TransferStatus::Skipped { .. }));
    assert_eq!(report.bytes_checked, size);

    let _ = shutdown_tx.send(());
    let _ = server_handle.await;
}

/// In-process: transfer a file, corrupt the received copy, then run --update. Server must re-transfer (not skip) because on-disk hash no longer matches; confirms BLAKE3 verification and cache invalidation.
#[tokio::test]
async fn test_loopback_detects_corruption() {
    let out_dir = tempdir().expect("temp dir");
    let out_path = out_dir.path().to_path_buf();
    let src_dir = tempdir().expect("temp dir for src");
    let src_path = src_dir.path().join("corrupt_me.bin");
    let size = 16 * 1024u64;
    let content: Vec<u8> = (0..size as usize).map(|i| (i % 251) as u8).collect();
    std::fs::write(&src_path, &content).expect("write");

    let (port_tx, port_rx) = tokio::sync::oneshot::channel();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server = TcpServer::new(0, out_path.clone(), transfer_config(0));
    let server_handle = tokio::spawn(async move {
        tokio::select! {
            _ = server.run_forever(Some(port_tx)) => {}
            _ = shutdown_rx => {}
        }
    });

    let port = port_rx.await.expect("server should send port");
    tokio::time::sleep(Duration::from_millis(100)).await;
    let config = transfer_config(port);
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().expect("parse addr");
    let transport = create_transport(&config, true).await;

    let sent1 = send_file_over_transport(
        transport.as_ref(),
        &src_path,
        addr,
        config.clone(),
        None,
        false,
        None,
    )
    .await
    .expect("first transfer");
    assert_eq!(sent1, size);

    let received_path = out_path.join("corrupt_me.bin");
    assert!(received_path.exists());
    let mut corrupted = std::fs::read(&received_path).expect("read received");
    if corrupted.len() >= 100 {
        corrupted[50] = corrupted[50].wrapping_add(1);
    }
    std::fs::write(&received_path, &corrupted).expect("write corrupted");

    let mut report = shift::TransferReport::default();
    let sent2 = send_file_over_transport(
        transport.as_ref(),
        &src_path,
        addr,
        config.clone(),
        Some(&mut report),
        true,
        None,
    )
    .await
    .expect("second transfer (re-send after corruption)");
    assert!(sent2 > 0, "should re-transfer after corruption, not skip");
    assert!(!matches!(report.status, shift::TransferStatus::Skipped { .. }), "should not skip corrupted file");

    let received_hash = hash_file(&received_path, size).expect("hash after re-transfer");
    let expected_hash = hash_file(&src_path, size).expect("hash source");
    assert_eq!(received_hash[..], expected_hash[..], "re-transferred file should match source");

    let _ = shutdown_tx.send(());
    let _ = server_handle.await;
}
