//! Server-side persistent cache of completed file hashes for --update skip checks.
//!
//! Uses a separate SQLite DB at ~/.shift/server_cache.db so it survives restarts
//! and is not tied to ephemeral transfer checkpoints.

use crate::error::TransferError;
use crate::integrity::BLAKE3_LEN;
use rusqlite::Connection;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

const FILE_HASHES_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS file_hashes (
    path TEXT PRIMARY KEY,
    blake3_hash BLOB NOT NULL,
    completed_at INTEGER NOT NULL
);
";

/// Returns the default path for the server cache DB: ~/.shift/server_cache.db.
/// Falls back to ./.shift/server_cache.db if HOME is not set.
pub fn server_cache_path() -> PathBuf {
    let base = std::env::var("HOME")
        .ok()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    base.join(".shift").join("server_cache.db")
}

/// Load all file hashes from the DB and spawn a background thread that persists
/// (path, hash) messages. Returns the initial map and a sender to use for persist.
/// On any error (open, load, or spawn), returns empty map and None sender so the
/// server still runs without persistence.
pub fn load_and_spawn_persist(
    db_path: &Path,
) -> (
    HashMap<PathBuf, [u8; BLAKE3_LEN]>,
    Option<mpsc::Sender<(PathBuf, [u8; BLAKE3_LEN])>>,
) {
    let (tx, rx) = mpsc::channel::<(PathBuf, [u8; BLAKE3_LEN])>();
    let path_for_thread = db_path.to_path_buf();

    let conn = match Connection::open(db_path) {
        Ok(c) => c,
        Err(e) => {
            tracing::debug!(path = %db_path.display(), error = %e, "Server cache DB open failed, --update skip will not persist across restarts");
            return (HashMap::new(), None);
        }
    };

    if let Err(e) = conn.execute_batch(FILE_HASHES_SCHEMA) {
        tracing::debug!(error = %e, "Server cache schema failed");
        return (HashMap::new(), None);
    }

    let map: HashMap<PathBuf, [u8; BLAKE3_LEN]> = {
        let mut stmt = match conn.prepare("SELECT path, blake3_hash FROM file_hashes") {
            Ok(s) => s,
            Err(e) => {
                tracing::debug!(error = %e, "Server cache prepare SELECT failed");
                return (HashMap::new(), None);
            }
        };
        stmt.query_map([], |row| {
            let path: String = row.get(0)?;
            let hash_vec: Vec<u8> = row.get(1)?;
            Ok((path, hash_vec))
        })
        .ok()
        .map(|rows| {
            rows.filter_map(|r| r.ok())
                .filter_map(|(path, hash_vec)| {
                    let arr: [u8; BLAKE3_LEN] = hash_vec.try_into().ok()?;
                    Some((PathBuf::from(path), arr))
                })
                .collect()
        })
        .unwrap_or_default()
    };

    thread::spawn(move || {
        let conn = match Connection::open(&path_for_thread) {
            Ok(c) => c,
            Err(e) => {
                tracing::debug!(error = %e, "Persist thread: DB open failed");
                return;
            }
        };
        if conn.execute_batch(FILE_HASHES_SCHEMA).is_err() {
            return;
        }
        while let Ok((path, hash)) = rx.recv() {
            let path_str = path.to_string_lossy().to_string();
            let completed_at = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;
            if conn
                .execute(
                    "INSERT OR REPLACE INTO file_hashes (path, blake3_hash, completed_at) VALUES (?1, ?2, ?3)",
                    rusqlite::params![path_str, hash.as_slice(), completed_at],
                )
                .is_err()
            {
                tracing::debug!(path = %path.display(), "Persist thread: INSERT failed");
            }
        }
    });

    (map, Some(tx))
}

/// Ensure the parent directory for the cache DB exists (e.g. ~/.shift).
/// Call this before load_and_spawn_persist so the first run creates the dir.
pub fn ensure_cache_dir(db_path: &Path) -> Result<(), TransferError> {
    if let Some(parent) = db_path.parent() {
        std::fs::create_dir_all(parent).map_err(TransferError::Io)?;
    }
    Ok(())
}
