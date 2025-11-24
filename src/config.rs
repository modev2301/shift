//! Configuration management for Shift.
//!
//! This module handles loading, saving, and managing configuration for both
//! the server and client components. Configuration is stored in TOML format
//! and includes settings for performance, security, and transfer behavior.

use crate::error::TransferError;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;

// Performance tuning constants
// Default chunk size balances protocol overhead and memory usage.
// Larger chunks reduce protocol overhead but increase memory requirements.
pub const BASE_CHUNK_SIZE: usize = 1024 * 1024; // 1MB
pub const DEFAULT_PARALLEL_STREAMS: usize = 16;
pub const MEMORY_POOL_SIZE: usize = 2000;
pub const SIMD_CHUNK_SIZE: usize = 1024 * 1024; // 1MB for SIMD operations
pub const ZERO_COPY_ENABLED: bool = true;

// Network constants
pub const DEFAULT_TIMEOUT_SECONDS: u64 = 30;
pub const MAX_RETRY_ATTEMPTS: u32 = 3;
pub const RETRY_DELAY_MS: u64 = 1000;
pub const CONNECTION_POOL_SIZE: usize = 100;

// Security constants
pub const DEFAULT_AUTH_TOKEN: &str = "shift_default_token";

// Compression constants
pub const COMPRESSION_THRESHOLD: usize = 1024; // Only compress chunks larger than 1KB
pub const LZ4_COMPRESSION_LEVEL: u32 = 1; // Fast compression
pub const ZSTD_COMPRESSION_LEVEL: i32 = 3; // Balanced compression

// Monitoring constants
pub const METRICS_ENABLED: bool = true;
pub const PROGRESS_BAR_ENABLED: bool = true;
pub const DETAILED_LOGGING: bool = true;

/// Main configuration structure containing all subsystem configurations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Server-specific configuration.
    pub server: ServerConfig,
    /// Client-specific configuration.
    pub client: ClientConfig,
    /// Security and authentication configuration.
    pub security: SecurityConfig,
    /// Performance tuning and optimization configuration.
    pub performance: PerformanceConfig,
}

/// Configuration for the transfer server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub address: String,
    pub port: u16,
    pub num_connections: Option<usize>,
    pub output_directory: String,
    pub max_clients: usize,
    pub parallel_streams: Option<usize>,
    pub buffer_size: Option<usize>,
    pub enable_progress_bar: bool,
    pub enable_compression: bool,
    pub enable_resume: bool,
    pub timeout_seconds: u64,
    pub max_file_size: Option<u64>,
    pub allowed_extensions: Option<Vec<String>>,
}

/// Configuration for the transfer client.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientConfig {
    pub server_address: String,
    pub server_port: u16,
    pub num_connections: Option<usize>,
    pub parallel_streams: Option<usize>,
    pub chunk_size: Option<usize>,
    pub buffer_size: Option<usize>,
    pub enable_compression: bool,
    pub enable_resume: bool,
    pub timeout_seconds: u64,
    pub retry_attempts: u32,
    pub retry_delay_ms: u64,
    pub progress_bar_enabled: bool,
    pub detailed_logging: bool,
}

/// Security and authentication configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityConfig {
    pub auth_token: String,
    pub max_connections_per_ip: Option<usize>,
}

/// Performance tuning and optimization configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceConfig {
    pub simd_enabled: bool,
    pub zero_copy_enabled: bool,
    pub memory_pool_size: usize,
    pub connection_pool_size: usize,
    pub compression_level: u32,
    pub metrics_enabled: bool,
}

impl Config {
    /// Loads configuration from a file, or creates a new default configuration
    /// if the file doesn't exist.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the configuration file (TOML format)
    ///
    /// # Returns
    ///
    /// Returns the loaded or newly created configuration, or an error if
    /// the file exists but cannot be read or parsed.
    pub fn load_or_create(path: &PathBuf) -> Result<Self, TransferError> {
        if path.exists() {
            let content = fs::read_to_string(path).map_err(|e| TransferError::Io(e))?;
            toml::from_str(&content).map_err(|e| TransferError::TomlDeserialization(e))
        } else {
            let config = Self::default();
            config.save(path)?;
            Ok(config)
        }
    }

    /// Saves the configuration to a file in TOML format.
    ///
    /// # Arguments
    ///
    /// * `path` - Path where the configuration should be saved
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the file cannot be written.
    pub fn save(&self, path: &PathBuf) -> Result<(), TransferError> {
        let content =
            toml::to_string_pretty(self).map_err(|e| TransferError::TomlSerialization(e))?;
        fs::write(path, content).map_err(|e| TransferError::Io(e))?;
        Ok(())
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            client: ClientConfig::default(),
            security: SecurityConfig::default(),
            performance: PerformanceConfig::default(),
        }
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            address: "127.0.0.1".to_string(),
            port: 8080,
            output_directory: "./downloads".to_string(),
            max_clients: 100,
            num_connections: Some(8),
            parallel_streams: Some(DEFAULT_PARALLEL_STREAMS),
            buffer_size: Some(BASE_CHUNK_SIZE),
            enable_progress_bar: PROGRESS_BAR_ENABLED,
            enable_compression: true,
            enable_resume: true,
            timeout_seconds: DEFAULT_TIMEOUT_SECONDS,
            max_file_size: Some(1024 * 1024 * 1024), // 1GB
            allowed_extensions: Some(vec!["*".to_string()]),
        }
    }
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            server_address: "127.0.0.1".to_string(),
            server_port: 8080,
            num_connections: Some(8),
            parallel_streams: Some(DEFAULT_PARALLEL_STREAMS),
            chunk_size: Some(BASE_CHUNK_SIZE),
            buffer_size: Some(BASE_CHUNK_SIZE),
            enable_compression: true,
            enable_resume: true,
            timeout_seconds: DEFAULT_TIMEOUT_SECONDS,
            retry_attempts: MAX_RETRY_ATTEMPTS,
            retry_delay_ms: RETRY_DELAY_MS,
            progress_bar_enabled: PROGRESS_BAR_ENABLED,
            detailed_logging: DETAILED_LOGGING,
        }
    }
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            auth_token: DEFAULT_AUTH_TOKEN.to_string(),
            max_connections_per_ip: Some(10),
        }
    }
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            simd_enabled: true,
            zero_copy_enabled: ZERO_COPY_ENABLED,
            memory_pool_size: MEMORY_POOL_SIZE,
            connection_pool_size: CONNECTION_POOL_SIZE,
            compression_level: LZ4_COMPRESSION_LEVEL,
            metrics_enabled: METRICS_ENABLED,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_config_default() {
        let config = Config::default();

        assert_eq!(config.server.address, "127.0.0.1");
        assert_eq!(config.server.port, 8080);
        assert_eq!(config.client.server_address, "127.0.0.1");
        assert_eq!(config.client.server_port, 8080);
        assert_eq!(config.security.auth_token, DEFAULT_AUTH_TOKEN);
        assert!(config.performance.simd_enabled);
    }

    #[test]
    fn test_server_config_default() {
        let config = ServerConfig::default();

        assert_eq!(config.address, "127.0.0.1");
        assert_eq!(config.port, 8080);
        assert_eq!(config.output_directory, "./downloads");
        assert_eq!(config.max_clients, 100);
        assert_eq!(config.parallel_streams, Some(DEFAULT_PARALLEL_STREAMS));
        assert_eq!(config.buffer_size, Some(BASE_CHUNK_SIZE));
        assert!(config.enable_progress_bar);
        assert!(config.enable_compression);
        assert!(config.enable_resume);
        assert_eq!(config.timeout_seconds, DEFAULT_TIMEOUT_SECONDS);
    }

    #[test]
    fn test_client_config_default() {
        let config = ClientConfig::default();

        assert_eq!(config.server_address, "127.0.0.1");
        assert_eq!(config.server_port, 8080);
        assert_eq!(config.parallel_streams, Some(DEFAULT_PARALLEL_STREAMS));
        assert_eq!(config.chunk_size, Some(BASE_CHUNK_SIZE));
        assert_eq!(config.buffer_size, Some(BASE_CHUNK_SIZE));
        assert!(config.enable_compression);
        assert!(config.enable_resume);
        assert_eq!(config.timeout_seconds, DEFAULT_TIMEOUT_SECONDS);
        assert_eq!(config.retry_attempts, MAX_RETRY_ATTEMPTS);
        assert_eq!(config.retry_delay_ms, RETRY_DELAY_MS);
    }

    #[test]
    fn test_security_config_default() {
        let config = SecurityConfig::default();

        assert_eq!(config.auth_token, DEFAULT_AUTH_TOKEN);
        assert_eq!(config.max_connections_per_ip, Some(10));
    }

    #[test]
    fn test_performance_config_default() {
        let config = PerformanceConfig::default();

        assert!(config.simd_enabled);
        assert!(config.zero_copy_enabled);
        assert_eq!(config.memory_pool_size, MEMORY_POOL_SIZE);
        assert_eq!(config.connection_pool_size, CONNECTION_POOL_SIZE);
        assert_eq!(config.compression_level, LZ4_COMPRESSION_LEVEL);
        assert!(config.metrics_enabled);
    }

    #[test]
    fn test_config_serialization() {
        let config = Config::default();
        let serialized = toml::to_string(&config).unwrap();
        let deserialized: Config = toml::from_str(&serialized).unwrap();

        assert_eq!(config.server.address, deserialized.server.address);
        assert_eq!(config.server.port, deserialized.server.port);
        assert_eq!(
            config.client.server_address,
            deserialized.client.server_address
        );
        assert_eq!(config.client.server_port, deserialized.client.server_port);
    }

    #[test]
    fn test_config_save_and_load() {
        let temp_dir = tempdir().unwrap();
        let config_path = temp_dir.path().join("test_config.toml");

        let original_config = Config::default();
        original_config.save(&config_path).unwrap();

        let loaded_config = Config::load_or_create(&config_path).unwrap();

        assert_eq!(original_config.server.address, loaded_config.server.address);
        assert_eq!(original_config.server.port, loaded_config.server.port);
        assert_eq!(
            original_config.client.server_address,
            loaded_config.client.server_address
        );
        assert_eq!(
            original_config.client.server_port,
            loaded_config.client.server_port
        );
    }

    #[test]
    fn test_config_create_new() {
        let temp_dir = tempdir().unwrap();
        let config_path = temp_dir.path().join("new_config.toml");

        // Should create new config file
        let config = Config::load_or_create(&config_path).unwrap();

        assert!(config_path.exists());
        assert_eq!(config.server.address, "127.0.0.1");
        assert_eq!(config.server.port, 8080);
    }

    #[test]
    fn test_constants() {
        assert_eq!(BASE_CHUNK_SIZE, 1024 * 1024); // 1MB
        assert_eq!(DEFAULT_PARALLEL_STREAMS, 16);
        assert_eq!(MEMORY_POOL_SIZE, 2000);
        assert_eq!(SIMD_CHUNK_SIZE, 1024 * 1024);
        assert!(ZERO_COPY_ENABLED);
        assert_eq!(DEFAULT_TIMEOUT_SECONDS, 30);
        assert_eq!(MAX_RETRY_ATTEMPTS, 3);
        assert_eq!(RETRY_DELAY_MS, 1000);
        assert_eq!(CONNECTION_POOL_SIZE, 100);
        assert_eq!(DEFAULT_AUTH_TOKEN, "shift_default_token");
        assert_eq!(COMPRESSION_THRESHOLD, 1024);
        assert_eq!(LZ4_COMPRESSION_LEVEL, 1);
        assert_eq!(ZSTD_COMPRESSION_LEVEL, 3);
        assert!(METRICS_ENABLED);
        assert!(PROGRESS_BAR_ENABLED);
        assert!(DETAILED_LOGGING);
    }

    #[test]
    fn test_custom_config() {
        let mut config = Config::default();
        config.server.address = "0.0.0.0".to_string();
        config.server.port = 9000;
        config.client.server_address = "192.168.1.100".to_string();
        config.client.server_port = 9000;
        config.security.auth_token = "custom_token".to_string();
        config.performance.simd_enabled = false;

        assert_eq!(config.server.address, "0.0.0.0");
        assert_eq!(config.server.port, 9000);
        assert_eq!(config.client.server_address, "192.168.1.100");
        assert_eq!(config.client.server_port, 9000);
        assert_eq!(config.security.auth_token, "custom_token");
        assert!(!config.performance.simd_enabled);
    }
}
