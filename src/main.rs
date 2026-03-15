use clap::{Parser, Subcommand};
use shift::config::{DEFAULT_PARALLEL_STREAMS, DEFAULT_TIMEOUT_SECONDS};
use shift::{TransferReport, TransferStatus};
use shift::Config;
use std::path::PathBuf;
use tracing::info;

#[derive(Parser)]
#[command(name = "shift")]
#[command(about = "High-performance file transfer tool")]
#[command(subcommand_negates_reqs = true)]
struct Cli {
    /// Recursive directory transfer
    #[arg(short = 'r', long)]
    recursive: bool,
    
    /// Skip files that haven't changed (hash comparison). Default: on; use --force to transfer anyway.
    #[arg(short = 'u', long = "update", default_value_t = true)]
    skip_unchanged: bool,

    /// Transfer even if file is unchanged on receiver
    #[arg(long = "force")]
    force: bool,
    
    /// Enable block deduplication (rsync-style)
    #[arg(short = 'd', long = "dedup")]
    deduplicate: bool,
    
    /// Force TCP transport (use when comparing with QUIC or if QUIC is unavailable)
    #[arg(long = "tcp")]
    force_tcp: bool,

    /// Number of parallel streams (default: auto from file size). Use 4 to limit to 5 server ports (8080–8084).
    #[arg(long = "streams", value_name = "N")]
    streams: Option<usize>,

    /// Use mutual TLS for TCP (metadata + data). Cert dir from --tls-dir or env TLS_CERT_DIR or ./tls-certs. Requires build with --features tls.
    #[arg(long = "tls")]
    tls: bool,

    /// Directory for TLS certs (ca.pem, server.pem, client.pem, etc.). Used when --tls is set. Default: env TLS_CERT_DIR or ./tls-certs.
    #[arg(long = "tls-dir", value_name = "DIR")]
    tls_dir: Option<PathBuf>,

    /// Print transfer stats after each file (human-readable)
    #[arg(long = "stats")]
    stats: bool,

    /// Print transfer stats as JSON (for scripting/benchmarks)
    #[arg(long = "json")]
    json: bool,

    /// Configuration file path
    #[arg(short, long, default_value = "config.toml")]
    config: PathBuf,
    
    #[command(subcommand)]
    command: Option<Commands>,
    
    /// Source and destination: SOURCE... DEST (last arg is destination, e.g. user@host:/path)
    #[arg(value_name = "PATH", num_args = 2..)]
    paths: Vec<String>,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the transfer server (TCP or QUIC; use --quic for QUIC)
    Server {
        /// Listen with QUIC (UDP) instead of TCP
        #[arg(long)]
        quic: bool,
    },
    /// Generate TLS certs for mutual auth (ca.pem, server.pem, client.pem, etc.) into DIR. Requires build with --features tls.
    TlsKeygen {
        /// Output directory for PEM files [default: ./tls-certs]
        #[arg(default_value = "./tls-certs")]
        dir: PathBuf,
    },
}

/// Parses SCP-like remote path: user@host:/path or host:/path
#[derive(Debug, Clone)]
struct RemotePath {
    _user: Option<String>,
    host: String,
    port: Option<u16>,
    path: PathBuf,
}

impl RemotePath {
    fn parse(s: &str) -> Result<Self, String> {
        // Format: [user@]host[:port]:/path
        if !s.contains(':') {
            return Err("Remote path must contain ':' separator".to_string());
        }
        
        let colon_pos = s.rfind(':').ok_or("Invalid remote path format")?;
        let path_str = &s[colon_pos + 1..];
        
        if !path_str.starts_with('/') {
            return Err("Remote path must be absolute (start with /)".to_string());
        }
        
        let remote_part = &s[..colon_pos];
        let (user_host, port) = if let Some(port_colon) = remote_part.rfind(':') {
            let port_str = &remote_part[port_colon + 1..];
            let port = port_str.parse::<u16>()
                .map_err(|_| format!("Invalid port: {}", port_str))?;
            (&remote_part[..port_colon], Some(port))
        } else {
            (remote_part, None)
        };
        
        let (user, host) = if let Some(at_pos) = user_host.find('@') {
            (Some(user_host[..at_pos].to_string()), user_host[at_pos + 1..].to_string())
        } else {
            (None, user_host.to_string())
        };
        
        Ok(RemotePath {
            _user: user,
            host,
            port,
            path: PathBuf::from(path_str),
        })
    }
    
}

/// Determines if a path string represents a remote path
fn is_remote_path(s: &str) -> bool {
    s.contains(':') && (s.contains('@') || s.matches(':').count() >= 2)
}

/// Resolve TLS cert directory when --tls is set: --tls-dir, or env TLS_CERT_DIR, or ./tls-certs.
fn tls_cert_dir(cli: &Cli) -> Option<PathBuf> {
    if !cli.tls {
        return None;
    }
    Some(
        cli.tls_dir.clone()
            .or_else(|| std::env::var("TLS_CERT_DIR").ok().map(PathBuf::from))
            .unwrap_or_else(|| PathBuf::from("./tls-certs")),
    )
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing with info level by default, but allow RUST_LOG env var to override
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .init();
    
    let cli = Cli::parse();
    
    // Handle explicit commands first
    if let Some(ref command) = cli.command {
        match command {
            Commands::TlsKeygen { dir } => {
                shift::tls::keygen(&dir)?;
                eprintln!("TLS certs written to {}", dir.display());
                return Ok(());
            }
            Commands::Server { quic } => {
                let config = Config::load_or_create(&cli.config)?;
                use shift::TransferConfig;
                let num_streams = config.server.parallel_streams.unwrap_or(DEFAULT_PARALLEL_STREAMS);
                use shift::utils::calculate_optimal_buffer_size;
                let buffer_size = config.server.buffer_size.unwrap_or_else(|| {
                    calculate_optimal_buffer_size(1024 * 1024 * 1024, num_streams)
                });
                let socket_buffer = config.server.socket_send_buffer_size
                    .or(config.server.socket_recv_buffer_size)
                    .unwrap_or_else(|| calculate_optimal_buffer_size(1024 * 1024 * 1024, num_streams));
                let transfer_config = TransferConfig {
                    start_port: config.server.port,
                    num_streams,
                    max_streams: num_streams,
                    buffer_size,
                    socket_send_buffer_size: Some(socket_buffer),
                    socket_recv_buffer_size: Some(socket_buffer),
                    enable_compression: config.server.enable_compression,
                    enable_encryption: false,
                    encryption_key: None,
                    timeout_seconds: DEFAULT_TIMEOUT_SECONDS,
                    tls_cert_dir: tls_cert_dir(&cli),
                    enable_fec: false,
                    fec_block_size: 65536,
                    fec_repair_packets: 4,
                    #[cfg(feature = "fec")]
                    fec_negotiated: false,
                };
                eprintln!("Shift Transfer Server");
                eprintln!("Port: {}", transfer_config.start_port);
                eprintln!("Output directory: {}", config.server.output_directory);
                if transfer_config.tls_cert_dir.is_some() {
                    eprintln!("TLS: mutual auth enabled (cert dir: {:?})", transfer_config.tls_cert_dir);
                }
                let rt = tokio::runtime::Runtime::new()?;
                if *quic {
                    use shift::quic_server::QuicServer;
                    eprintln!("Transport: QUIC (UDP)");
                    let server = QuicServer::new(
                        transfer_config.start_port,
                        PathBuf::from(&config.server.output_directory),
                        transfer_config,
                    );
                    rt.block_on(server.run_forever())?;
                } else {
                    use shift::tcp_server::TcpServer;
                    eprintln!("Transport: TCP");
                    let server = TcpServer::new(
                        transfer_config.start_port,
                        PathBuf::from(&config.server.output_directory),
                        transfer_config,
                    );
                    rt.block_on(server.run_forever(None))?;
                }
                return Ok(());
            }
        }
    }
    
    // Handle SCP-like syntax: shift source... dest (last arg is destination)
    if cli.paths.len() < 2 {
        eprintln!("Error: At least one source and one destination required");
        eprintln!("Usage: shift [OPTIONS] SOURCE... DEST");
        eprintln!("       shift [OPTIONS] file1.txt file2.txt user@host:/path/");
        eprintln!("       shift [OPTIONS] *.log host:/backup/");
        eprintln!("       shift [OPTIONS] user@host:/file.txt ./");
        std::process::exit(1);
    }
    let (sources, dest) = {
        let (last, rest) = cli.paths.split_last().unwrap();
        (rest.to_vec(), last.clone())
    };
    
    // Check if any source is remote
    let source_is_remote = sources.iter().any(|s| is_remote_path(s));
    let dest_is_remote = is_remote_path(&dest);
    
    if source_is_remote && dest_is_remote {
        return Err("Cannot transfer between two remote locations".into());
    }
    
    let config = Config::load_or_create(&cli.config)?;
    
    if source_is_remote {
        // Pull mode: shift user@host:/file.txt ./
        if sources.len() > 1 {
            return Err("Pull mode supports only one source file at a time".into());
        }
        let remote = RemotePath::parse(&sources[0])?;
        
        info!("Pulling {} from {}:{}", remote.path.display(), remote.host, remote.port.unwrap_or(443));
        
        return Err("Pull mode requires server-side file listing and serving. Not yet implemented.".into());
    } else if dest_is_remote {
        // Push mode: shift file1.txt file2.txt user@host:/path/ or shift *.log host:/backup/
        let remote = RemotePath::parse(&dest)?;
        
        // Update config with remote host/port
        let mut config = config;
        config.client.server_address = remote.host.clone();
        config.client.server_port = remote.port.unwrap_or(443);
        
        // Collect all source files (handle glob patterns and multiple files)
        let mut all_sources = Vec::new();
        for source in sources {
            if source.contains('*') || source.contains('?') {
                // Glob pattern
                let matches = glob::glob(&source)?
                    .collect::<Result<Vec<_>, _>>()?;
                all_sources.extend(matches);
            } else {
                // Direct file/directory path
                all_sources.push(PathBuf::from(&source));
            }
        }
        
        if all_sources.is_empty() {
            return Err("No files to transfer".into());
        }
        
        // Check if any source is a directory
        let has_directories = all_sources.iter().any(|p| p.is_dir());
        
        if has_directories && !cli.recursive {
            return Err("Source contains directories. Use -r for recursive transfer".into());
        }
        
        // Collect all files to transfer
        let mut files_to_transfer = Vec::new();
        for source_path in all_sources {
            if source_path.is_dir() && cli.recursive {
                // Add all files in directory
                for entry in walkdir::WalkDir::new(&source_path) {
                    let entry = entry?;
                    if entry.file_type().is_file() {
                        files_to_transfer.push(entry.path().to_path_buf());
                    }
                }
            } else if source_path.is_file() {
                files_to_transfer.push(source_path);
            }
        }
        
        if files_to_transfer.is_empty() {
            return Err("No files to transfer".into());
        }
        
        if files_to_transfer.len() > 1 {
            // Only show file count for multiple files
        if files_to_transfer.len() > 1 {
            eprintln!("Transferring {} files", files_to_transfer.len());
        }
        }
        
        use shift::tcp_transfer::send_file_over_transport;
        use shift::transport::create_transport;
        use shift::TransferConfig;
        use shift::utils::calculate_optimal_parallel_streams;
        use std::net::ToSocketAddrs;
        
        // Stream count: CLI --streams overrides config, else auto from file size (fewer streams = fewer server ports)
        let num_streams = cli.streams
            .or(config.client.parallel_streams)
            .unwrap_or_else(|| {
                let total_size: u64 = files_to_transfer.iter()
                    .filter_map(|path| std::fs::metadata(path).ok().map(|m| m.len()))
                    .sum();
                if total_size > 0 {
                    calculate_optimal_parallel_streams(total_size, None)
                } else {
                    DEFAULT_PARALLEL_STREAMS
                }
            });
        
        // Auto-calculate buffer sizes based on file size if not specified
        use shift::utils::calculate_optimal_buffer_size;
        let total_size: u64 = files_to_transfer.iter()
            .filter_map(|path| std::fs::metadata(path).ok().map(|m| m.len()))
            .sum();
        
        let buffer_size = config.client.buffer_size.unwrap_or_else(|| {
            if total_size > 0 {
                calculate_optimal_buffer_size(total_size, num_streams)
            } else {
                16 * 1024 * 1024 // Default fallback
            }
        });
        
        let socket_buffer = config.client.socket_send_buffer_size
            .or(config.client.socket_recv_buffer_size)
            .unwrap_or_else(|| {
                if total_size > 0 {
                    calculate_optimal_buffer_size(total_size, num_streams)
                } else {
                    16 * 1024 * 1024 // Default fallback
                }
            });
        
        // When user passed --streams, do not scale up (keeps server port range small). Otherwise allow scale-up.
        let max_streams = if cli.streams.is_some() {
            num_streams
        } else {
            (num_streams * 2).max(32).min(256)
        };
        let transfer_config = TransferConfig {
            start_port: remote.port.unwrap_or(8080),
            num_streams,
            max_streams,
            buffer_size,
            socket_send_buffer_size: Some(socket_buffer),
            socket_recv_buffer_size: Some(socket_buffer),
            enable_compression: config.client.enable_compression,
            enable_encryption: false,
            encryption_key: None,
            timeout_seconds: DEFAULT_TIMEOUT_SECONDS,
            tls_cert_dir: tls_cert_dir(&cli),
            enable_fec: false,
            fec_block_size: 65536,
            fec_repair_packets: 4,
            #[cfg(feature = "fec")]
            fec_negotiated: false,
        };
        
        // Sort files by size (largest first) for better parallelism utilization
        files_to_transfer.sort_by(|a, b| {
            let size_a = std::fs::metadata(a).ok().map(|m| m.len()).unwrap_or(0);
            let size_b = std::fs::metadata(b).ok().map(|m| m.len()).unwrap_or(0);
            size_b.cmp(&size_a)
        });
        
        let rt = tokio::runtime::Runtime::new()?;
        let transport = rt.block_on(create_transport(&transfer_config, cli.force_tcp));
        let output_stats = cli.stats || cli.json;
        let mut report = TransferReport::default();

        for (idx, local_file) in files_to_transfer.iter().enumerate() {
            let file_idx = idx + 1;
            let total_files = files_to_transfer.len();

            if total_files > 1 {
                eprintln!("[{}/{}] {}", file_idx, total_files, local_file.display());
            }

            let server_addr_str = format!("{}:{}", remote.host, remote.port.unwrap_or(8080));
            let server_addr = server_addr_str
                .to_socket_addrs()?
                .next()
                .ok_or_else(|| "Failed to resolve server address")?;

            let stats_out = if output_stats { Some(&mut report) } else { None };
            let skip_unchanged = cli.skip_unchanged && !cli.force;
            match rt.block_on(send_file_over_transport(
                transport.as_ref(),
                local_file,
                server_addr,
                transfer_config.clone(),
                stats_out,
                skip_unchanged,
                None,
            )) {
                Ok(_) => {
                    if cli.json {
                        println!("{}", serde_json::to_string_pretty(&report).unwrap_or_else(|_| "{}".into()));
                    } else if cli.stats {
                        if matches!(report.status, TransferStatus::Skipped { .. }) {
                            let gb = report.bytes_checked as f64 / (1024.0 * 1024.0 * 1024.0);
                            let size_str = if gb >= 1.0 {
                                format!("{:.2} GB", gb)
                            } else {
                                format!("{} bytes", report.bytes_checked)
                            };
                            eprintln!(
                                "{}  skipped (unchanged)  {} checked  {}ms",
                                local_file.file_name().map(|n| n.to_string_lossy()).unwrap_or_default(),
                                size_str,
                                report.duration_ms,
                            );
                        } else {
                            eprintln!(
                                "transport: {}  bytes: {}  duration_ms: {}  throughput_mbps: {:.2}  streams: {}→{}  stalled: {}  rtt_ms: {}  ranges: {} total, {} resumed",
                                report.transport,
                                report.bytes,
                                report.duration_ms,
                                report.throughput_mbps,
                                report.streams_initial,
                                report.streams_peak,
                                report.streams_stalled,
                                report.rtt_ms,
                                report.ranges_total,
                                report.ranges_resumed,
                            );
                        }
                    }
                }
                Err(e) => {
                    eprintln!("[{}/{}] Transfer failed: {} - {}", file_idx, total_files, local_file.display(), e);
                }
            }
        }
        
        // Progress bars already show completion
    } else {
        return Err("At least one path must be remote (user@host:/path)".into());
    }
    
    Ok(())
}


