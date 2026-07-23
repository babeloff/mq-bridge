//  mq-bridge
//  © Copyright 2026, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/mq-bridge
//
//! Shared AEAD core for the `encryption` middleware and the at-rest encryption
//! of the file / object_store endpoints.
//!
//! `seal` produces a self-describing envelope so `open` needs no out-of-band
//! agreement beyond the keys:
//!
//! ```text
//! [version:u8=1][cipher:u8][key_id_len:u8][key_id][nonce][ciphertext‖tag]
//! ```
//!
//! A fresh random nonce is drawn per `seal`. The default XChaCha20-Poly1305
//! uses a 192-bit nonce, which is collision-safe with random nonces at any
//! realistic message rate; AES-256-GCM (96-bit nonce) is offered for
//! interoperability.

use crate::models::{CipherKind, EncryptionConfig};
use aes_gcm::Aes256Gcm;
use anyhow::{anyhow, Context};
use base64::Engine as _;
use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::XChaCha20Poly1305;
use std::collections::HashMap;

const ENVELOPE_VERSION: u8 = 1;
const CIPHER_XCHACHA: u8 = 0;
const CIPHER_AES_GCM: u8 = 1;
const XCHACHA_NONCE_LEN: usize = 24;
const AES_GCM_NONCE_LEN: usize = 12;

/// A ready-to-use AEAD engine built from an [`EncryptionConfig`]: the active
/// seal key plus any extra decrypt-only keys (rotation).
pub struct Crypto {
    cipher: CipherKind,
    key_id: String,
    key: [u8; 32],
    decrypt_keys: HashMap<String, [u8; 32]>,
}

/// Decodes a configured key: optional `${env:VAR}` indirection, then base64 to
/// exactly 32 bytes.
fn decode_key(configured: &str, key_id: &str) -> anyhow::Result<[u8; 32]> {
    let raw = match configured
        .strip_prefix("${env:")
        .and_then(|r| r.strip_suffix('}'))
    {
        Some(var) => std::env::var(var).with_context(|| {
            format!("environment variable '{var}' for encryption key '{key_id}' is not set")
        })?,
        None => configured.to_string(),
    };
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(raw.trim())
        .with_context(|| format!("encryption key '{key_id}' is not valid base64"))?;
    <[u8; 32]>::try_from(bytes.as_slice()).map_err(|_| {
        anyhow!(
            "encryption key '{key_id}' must be 32 bytes, got {}",
            bytes.len()
        )
    })
}

impl Crypto {
    pub fn new(config: &EncryptionConfig) -> anyhow::Result<Self> {
        if config.key_id.is_empty() || config.key_id.len() > u8::MAX as usize {
            return Err(anyhow!(
                "encryption key_id must be 1..=255 bytes, got {}",
                config.key_id.len()
            ));
        }
        let key = decode_key(&config.key, &config.key_id)?;
        let mut decrypt_keys = HashMap::new();
        for (id, k) in &config.decrypt_keys {
            decrypt_keys.insert(id.clone(), decode_key(k, id)?);
        }
        Ok(Self {
            cipher: config.cipher,
            key_id: config.key_id.clone(),
            key,
            decrypt_keys,
        })
    }

    /// Encrypts `plaintext` with the active key into a self-describing envelope.
    pub fn seal(&self, plaintext: &[u8], aad: &[u8]) -> anyhow::Result<Vec<u8>> {
        let (cipher_byte, nonce_len) = match self.cipher {
            CipherKind::Xchacha20poly1305 => (CIPHER_XCHACHA, XCHACHA_NONCE_LEN),
            CipherKind::Aes256gcm => (CIPHER_AES_GCM, AES_GCM_NONCE_LEN),
        };
        let nonce_bytes: [u8; XCHACHA_NONCE_LEN] = rand::random();
        let nonce = &nonce_bytes[..nonce_len];
        let payload = Payload {
            msg: plaintext,
            aad,
        };
        let ciphertext = match self.cipher {
            CipherKind::Xchacha20poly1305 => {
                XChaCha20Poly1305::new((&self.key).into()).encrypt((&nonce_bytes).into(), payload)
            }
            CipherKind::Aes256gcm => {
                let nonce: &[u8; AES_GCM_NONCE_LEN] =
                    nonce.try_into().expect("nonce length fixed above");
                Aes256Gcm::new((&self.key).into()).encrypt(nonce.into(), payload)
            }
        }
        .map_err(|_| anyhow!("AEAD encryption failed"))?;

        let mut out = Vec::with_capacity(3 + self.key_id.len() + nonce_len + ciphertext.len());
        out.push(ENVELOPE_VERSION);
        out.push(cipher_byte);
        out.push(self.key_id.len() as u8);
        out.extend_from_slice(self.key_id.as_bytes());
        out.extend_from_slice(nonce);
        out.extend_from_slice(&ciphertext);
        Ok(out)
    }

    /// Parses an envelope, selects the key by its `key_id`, and decrypts.
    /// Any parse, unknown-key, or authentication failure is a hard error.
    pub fn open(&self, envelope: &[u8], aad: &[u8]) -> anyhow::Result<Vec<u8>> {
        let err = || anyhow!("invalid encryption envelope");
        let (&version, rest) = envelope.split_first().ok_or_else(err)?;
        if version != ENVELOPE_VERSION {
            return Err(anyhow!("unsupported encryption envelope version {version}"));
        }
        let (&cipher_byte, rest) = rest.split_first().ok_or_else(err)?;
        let (&key_id_len, rest) = rest.split_first().ok_or_else(err)?;
        if rest.len() < key_id_len as usize {
            return Err(err());
        }
        let (key_id, rest) = rest.split_at(key_id_len as usize);
        let key_id = std::str::from_utf8(key_id).map_err(|_| err())?;
        let nonce_len = match cipher_byte {
            CIPHER_XCHACHA => XCHACHA_NONCE_LEN,
            CIPHER_AES_GCM => AES_GCM_NONCE_LEN,
            other => return Err(anyhow!("unknown encryption cipher id {other}")),
        };
        if rest.len() < nonce_len {
            return Err(err());
        }
        let (nonce, ciphertext) = rest.split_at(nonce_len);

        let key = if key_id == self.key_id {
            &self.key
        } else {
            self.decrypt_keys
                .get(key_id)
                .ok_or_else(|| anyhow!("no decryption key for key_id '{key_id}'"))?
        };
        let payload = Payload {
            msg: ciphertext,
            aad,
        };
        match cipher_byte {
            CIPHER_XCHACHA => {
                let nonce: &[u8; XCHACHA_NONCE_LEN] =
                    nonce.try_into().expect("nonce length checked above");
                XChaCha20Poly1305::new(key.into()).decrypt(nonce.into(), payload)
            }
            _ => {
                let nonce: &[u8; AES_GCM_NONCE_LEN] =
                    nonce.try_into().expect("nonce length checked above");
                Aes256Gcm::new(key.into()).decrypt(nonce.into(), payload)
            }
        }
        .map_err(|_| anyhow!("AEAD decryption failed (tampered data or wrong key)"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(cipher: CipherKind) -> EncryptionConfig {
        EncryptionConfig {
            cipher,
            key_id: "k1".to_string(),
            key: base64::engine::general_purpose::STANDARD.encode([7u8; 32]),
            decrypt_keys: HashMap::new(),
        }
    }

    #[test]
    fn seal_open_round_trip_both_ciphers() {
        for cipher in [CipherKind::Xchacha20poly1305, CipherKind::Aes256gcm] {
            let crypto = Crypto::new(&config(cipher)).unwrap();
            let envelope = crypto.seal(b"secret payload", b"aad").unwrap();
            assert_ne!(&envelope, b"secret payload");
            assert_eq!(crypto.open(&envelope, b"aad").unwrap(), b"secret payload");
        }
    }

    #[test]
    fn envelope_header_is_self_describing() {
        let crypto = Crypto::new(&config(CipherKind::Aes256gcm)).unwrap();
        let envelope = crypto.seal(b"x", b"").unwrap();
        assert_eq!(envelope[0], ENVELOPE_VERSION);
        assert_eq!(envelope[1], CIPHER_AES_GCM);
        assert_eq!(envelope[2], 2);
        assert_eq!(&envelope[3..5], b"k1");
    }

    #[test]
    fn bit_flip_and_wrong_key_fail() {
        let crypto = Crypto::new(&config(CipherKind::Xchacha20poly1305)).unwrap();
        let mut envelope = crypto.seal(b"secret", b"").unwrap();
        *envelope.last_mut().unwrap() ^= 1;
        assert!(crypto.open(&envelope, b"").is_err());

        let mut other_cfg = config(CipherKind::Xchacha20poly1305);
        other_cfg.key = base64::engine::general_purpose::STANDARD.encode([9u8; 32]);
        let other = Crypto::new(&other_cfg).unwrap();
        let envelope = crypto.seal(b"secret", b"").unwrap();
        assert!(other.open(&envelope, b"").is_err());
    }

    #[test]
    fn rotation_key_is_used_for_unknown_active_id() {
        let old = Crypto::new(&config(CipherKind::Xchacha20poly1305)).unwrap();
        let envelope = old.seal(b"rotated", b"").unwrap();

        let mut new_cfg = config(CipherKind::Xchacha20poly1305);
        new_cfg.key_id = "k2".to_string();
        new_cfg.key = base64::engine::general_purpose::STANDARD.encode([9u8; 32]);
        new_cfg
            .decrypt_keys
            .insert("k1".to_string(), config(CipherKind::Xchacha20poly1305).key);
        let new = Crypto::new(&new_cfg).unwrap();
        assert_eq!(new.open(&envelope, b"").unwrap(), b"rotated");
    }

    #[test]
    fn rejects_bad_keys() {
        let mut cfg = config(CipherKind::Xchacha20poly1305);
        cfg.key = "not base64!!".to_string();
        assert!(Crypto::new(&cfg).is_err());
        let mut cfg = config(CipherKind::Xchacha20poly1305);
        cfg.key = base64::engine::general_purpose::STANDARD.encode([1u8; 16]);
        assert!(Crypto::new(&cfg).is_err());
    }
}
