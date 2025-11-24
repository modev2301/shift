use clap::{Parser, Subcommand};
use indicatif::MultiProgress;
use shift::{ClientTransferManager, Config, PerformanceBenchmark, TransferServer};
use std::path::PathBuf;
use std::sync::Arc;
use tracing::info;

#[derive(Parser)]
#[command(name = "shift")]
#[command(about = "High-performance file transfer tool")]
struct Cli {
    /// Recursive directory transfer
    #[arg(short = 'r', long)]
    recursive: bool,
    
    /// Skip files that haven't changed (hash comparison)
    #[arg(short = 'u', long = "update")]
    skip_unchanged: bool,
    
    /// Enable block deduplication (rsync-style)
    #[arg(short = 'd', long = "dedup")]
    deduplicate: bool,
    
    /// Configuration file path
    #[arg(short, long, default_value = "config.toml")]
    config: PathBuf,
    
    #[command(subcommand)]
    command: Option<Commands>,
    
    /// Source paths (files, directories, or remote paths). Can specify multiple files.
    #[arg(value_name = "SOURCE", num_args = 1..)]
    sources: Vec<String>,
    
    /// Destination path (file, directory, or remote path)
    #[arg(value_name = "DEST")]
    dest: Option<String>,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the transfer server
    Server,
    /// Run performance benchmarks
    Benchmark {
        #[arg(short, long, default_value = "./benchmark_results")]
        output_dir: PathBuf,
    },
}

/// Parses SCP-like remote path: user@host:/path or host:/path
#[derive(Debug, Clone)]
struct RemotePath {
    user: Option<String>,
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
            user,
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    
    let cli = Cli::parse();
    
    // Handle explicit commands first
    if let Some(command) = cli.command {
        match command {
            Commands::Server => {
                info!("Starting server with config: {:?}", cli.config);
                let config = Config::load_or_create(&cli.config)?;
                let server = TransferServer::new(Arc::new(config.server));
                server.run().await?;
                return Ok(());
            }
            Commands::Benchmark { output_dir } => {
                info!("Running performance benchmarks");
                run_benchmarks(&output_dir).await?;
                return Ok(());
            }
        }
    }
    
    // Handle SCP-like syntax: shift source... dest
    if cli.sources.is_empty() || cli.dest.is_none() {
        eprintln!("Error: Source and destination required");
        eprintln!("Usage: shift [OPTIONS] SOURCE... DEST");
        eprintln!("       shift [OPTIONS] file1.txt file2.txt user@host:/path/");
        eprintln!("       shift [OPTIONS] *.log host:/backup/");
        eprintln!("       shift [OPTIONS] user@host:/file.txt ./");
        std::process::exit(1);
    }
    
    let dest = cli.dest.unwrap();
    let sources = cli.sources;
    
    // Check if any source is remote
    let source_is_remote = sources.iter().any(|s| is_remote_path(s));
    let dest_is_remote = is_remote_path(&dest);
    
    if source_is_remote && dest_is_remote {
        return Err("Cannot transfer between two remote locations".into());
    }
    
    let config = Config::load_or_create(&cli.config)?;
    let multi_progress = MultiProgress::new();
    
    if source_is_remote {
        // Pull mode: shift user@host:/file.txt ./
        if sources.len() > 1 {
            return Err("Pull mode supports only one source file at a time".into());
        }
        let remote = RemotePath::parse(&sources[0])?;
        let local_dest = PathBuf::from(&dest);
        
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
        
        info!("Transferring {} files", files_to_transfer.len());
        
        // Establish connections once and reuse them across all files
        // This avoids the overhead of connection establishment for each file
        info!("Establishing {} connections for connection pool...", config.client.num_connections.unwrap_or(8));
        let mut pooled_connections = Some(
            ClientTransferManager::establish_connections(&config.client).await?
        );
        info!("Connection pool established - will reuse for all {} files", files_to_transfer.len());
        
        // Transfer files sequentially, reusing the same connections
        // Each file gets a new session_id and handshake, but uses the same TCP connections
        for (idx, local_file) in files_to_transfer.iter().enumerate() {
            info!("[{}/{}] Transferring {} -> {}:{}", 
                idx + 1, files_to_transfer.len(), 
                local_file.display(), remote.host, remote.port.unwrap_or(443));
            
            let mut client_manager = ClientTransferManager::new(
                local_file.clone(),
                Arc::new(config.client.clone()),
                multi_progress.clone(),
            ).await?;
            
            // Reuse connections from pool when available
            // Connections are consumed during transfer, so we re-establish for subsequent files
            if let Some(conns) = pooled_connections.take() {
                client_manager.run_transfer_with_connections(Some(conns)).await?;
            } else {
                // Fallback: create new connections if pool was consumed
                client_manager.run_transfer().await?;
            }
            
            // Re-establish connections for next file (since they're consumed)
            // This is still better than creating connections inside ClientTransferManager
            // because we can batch the connection establishment
            if idx < files_to_transfer.len() - 1 {
                info!("Re-establishing connection pool for next file...");
                pooled_connections = Some(
                    ClientTransferManager::establish_connections(&config.client).await?
                );
            }
        }
        
        info!("All {} files transferred successfully", files_to_transfer.len());
    } else {
        return Err("At least one path must be remote (user@host:/path)".into());
    }
    
    Ok(())
}


async fn run_benchmarks(output_dir: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    std::fs::create_dir_all(output_dir)?;
    
    let mut benchmark = PerformanceBenchmark::new();
    
    println!("Starting performance benchmarks...");
    println!("Test files will be created in: {:?}", output_dir);
    
    let results = benchmark.run_full_benchmark(output_dir.to_str().unwrap())?;
    benchmark.print_results();
    
    let results_file = output_dir.join("benchmark_results.json");
    let json_results = serde_json::to_string_pretty(&results)?;
    std::fs::write(&results_file, json_results)?;
    println!("\nDetailed results saved to: {:?}", results_file);
    
    Ok(())
}
