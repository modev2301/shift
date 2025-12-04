//! Encryption support using AES-256-GCM.
//!
//! Provides authenticated encryption for file transfers using a pre-shared key (PSK).

use crate::error::TransferError;
use ring::aead::{Aad, BoundKey, Nonce, NonceSequence, OpeningKey, SealingKey, UnboundKey, AES_256_GCM};
use ring::rand::{SecureRandom, SystemRandom};

/// Nonce sequence for AES-GCM encryption.
struct CounterNonce {
    counter: u64,
    thread_id: u32,
}

impl CounterNonce {
    fn new(thread_id: u32) -> Self {
        Self {
            counter: 0,
            thread_id,
        }
    }
}

impl NonceSequence for CounterNonce {
    fn advance(&mut self) -> Result<Nonce, ring::error::Unspecified> {
        let mut nonce_bytes = [0u8; 12];
        nonce_bytes[0..4].copy_from_slice(&self.thread_id.to_le_bytes());
        nonce_bytes[4..12].copy_from_slice(&self.counter.to_le_bytes());
        self.counter += 1;
        Nonce::try_assume_unique_for_key(&nonce_bytes)
    }
}

/// Encryptor for file transfer data.
pub struct Encryptor {
    sealing_key: SealingKey<CounterNonce>,
}

impl Encryptor {
    /// Create a new encryptor from a pre-shared key.
    pub fn new(key: &[u8], thread_id: u32) -> Result<Self, TransferError> {
        if key.len() != 32 {
            return Err(TransferError::ProtocolError(
                "Encryption key must be 32 bytes (AES-256)".to_string()
            ));
        }

        let unbound_key = UnboundKey::new(&AES_256_GCM, key)
            .map_err(|e| TransferError::ProtocolError(format!("Failed to create encryption key: {:?}", e)))?;
        
        let nonce_sequence = CounterNonce::new(thread_id);
        let sealing_key = SealingKey::new(unbound_key, nonce_sequence);

        Ok(Self { sealing_key })
    }

    /// Encrypt data in-place.
    pub fn encrypt_in_place(&mut self, data: &mut Vec<u8>) -> Result<(), TransferError> {
        self.sealing_key.seal_in_place_append_tag(Aad::empty(), data)
            .map_err(|e| TransferError::ProtocolError(format!("Encryption failed: {:?}", e)))?;
        Ok(())
    }
}

/// Decryptor for file transfer data.
pub struct Decryptor {
    opening_key: OpeningKey<CounterNonce>,
}

impl Decryptor {
    /// Create a new decryptor from a pre-shared key.
    pub fn new(key: &[u8], thread_id: u32) -> Result<Self, TransferError> {
        if key.len() != 32 {
            return Err(TransferError::ProtocolError(
                "Encryption key must be 32 bytes (AES-256)".to_string()
            ));
        }

        let unbound_key = UnboundKey::new(&AES_256_GCM, key)
            .map_err(|e| TransferError::ProtocolError(format!("Failed to create decryption key: {:?}", e)))?;
        
        let nonce_sequence = CounterNonce::new(thread_id);
        let opening_key = OpeningKey::new(unbound_key, nonce_sequence);

        Ok(Self { opening_key })
    }

    /// Decrypt data in-place.
    pub fn decrypt_in_place(&mut self, data: &mut Vec<u8>) -> Result<usize, TransferError> {
        let plaintext_len = self.opening_key.open_in_place(Aad::empty(), data)
            .map_err(|e| TransferError::ProtocolError(format!("Decryption failed: {:?}", e)))?
            .len();
        Ok(plaintext_len)
    }
}

/// Generate a random encryption key.
pub fn generate_key() -> Result<[u8; 32], TransferError> {
    let rng = SystemRandom::new();
    let mut key = [0u8; 32];
    rng.fill(&mut key)
        .map_err(|e| TransferError::ProtocolError(format!("Failed to generate key: {:?}", e)))?;
    Ok(key)
}

/// Derive encryption key from a password using SHA-256.
pub fn derive_key_from_password(password: &str) -> Result<[u8; 32], TransferError> {
    use ring::digest::{digest, SHA256};
    
    // Simple key derivation: hash password + salt
    let mut input = Vec::new();
    input.extend_from_slice(b"shift-file-transfer-salt");
    input.extend_from_slice(password.as_bytes());
    input.extend_from_slice(b"shift-file-transfer-salt");
    
    let hash = digest(&SHA256, &input);
    let mut key = [0u8; 32];
    key.copy_from_slice(hash.as_ref());
    
    Ok(key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encrypt_decrypt() {
        let key = generate_key().unwrap();
        let mut encryptor = Encryptor::new(&key, 0).unwrap();
        let mut decryptor = Decryptor::new(&key, 0).unwrap();
        
        let mut data = b"Hello, world!".to_vec();
        encryptor.encrypt_in_place(&mut data).unwrap();
        
        let plaintext_len = decryptor.decrypt_in_place(&mut data).unwrap();
        data.truncate(plaintext_len);
        
        assert_eq!(data, b"Hello, world!");
    }

    #[test]
    fn test_key_derivation() {
        let key1 = derive_key_from_password("password123").unwrap();
        let key2 = derive_key_from_password("password123").unwrap();
        assert_eq!(key1, key2);
        
        let key3 = derive_key_from_password("different").unwrap();
        assert_ne!(key1, key3);
    }

    #[test]
    fn test_nonce_uniqueness() {
        let key = generate_key().unwrap();
        let mut enc1 = Encryptor::new(&key, 0).unwrap();
        let mut enc2 = Encryptor::new(&key, 1).unwrap();
        
        let mut data1 = b"test1".to_vec();
        let mut data2 = b"test2".to_vec();
        
        enc1.encrypt_in_place(&mut data1).unwrap();
        enc2.encrypt_in_place(&mut data2).unwrap();
        
        // Different nonces should produce different ciphertexts
        assert_ne!(data1, data2);
    }
}

