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
use chacha20poly1305::aead::{Aead, AeadInOut, KeyInit, Payload};
use chacha20poly1305::XChaCha20Poly1305;
use std::collections::HashMap;

use super::crypto_envelope::{
    AES_GCM_NONCE_LEN, CIPHER_AES_GCM, CIPHER_XCHACHA, ENVELOPE_VERSION, XCHACHA_NONCE_LEN,
};

/// Authentication tag length; 16 bytes for both ciphers.
const TAG_LEN: usize = 16;

/// The AEAD instance for one key, built once. AES-256's key schedule and GHASH
/// table cost more to set up than sealing a small payload, so they are not
/// rebuilt per message. XChaCha needs no key schedule but shares the shape.
enum Cipher {
    Xchacha(XChaCha20Poly1305),
    /// Boxed: the AES round keys and GHASH table are ~1 KiB, XChaCha's key is 32 bytes.
    Aes(Box<Aes256Gcm>),
}

impl Cipher {
    fn new(kind: CipherKind, key: &[u8; 32]) -> Self {
        match kind {
            CipherKind::Xchacha20poly1305 => Cipher::Xchacha(XChaCha20Poly1305::new(key.into())),
            CipherKind::Aes256gcm => Cipher::Aes(Box::new(Aes256Gcm::new(key.into()))),
        }
    }

    /// Encrypts `out[body..]` in place and appends the tag, so the ciphertext is
    /// written straight into the envelope rather than allocated and copied in.
    fn seal_in_place(
        &self,
        nonce: &[u8],
        aad: &[u8],
        out: &mut Vec<u8>,
        body: usize,
    ) -> anyhow::Result<()> {
        let failed = || anyhow!("AEAD encryption failed");
        let tag = match self {
            Cipher::Xchacha(cipher) => {
                let nonce: &[u8; XCHACHA_NONCE_LEN] = nonce.try_into().map_err(|_| failed())?;
                cipher.encrypt_inout_detached(nonce.into(), aad, (&mut out[body..]).into())
            }
            Cipher::Aes(cipher) => {
                let nonce: &[u8; AES_GCM_NONCE_LEN] = nonce.try_into().map_err(|_| failed())?;
                cipher.encrypt_inout_detached(nonce.into(), aad, (&mut out[body..]).into())
            }
        }
        .map_err(|_| failed())?;
        out.extend_from_slice(&tag);
        Ok(())
    }

    fn open(&self, nonce: &[u8], aad: &[u8], ciphertext: &[u8]) -> anyhow::Result<Vec<u8>> {
        let failed = || anyhow!("AEAD decryption failed (tampered data or wrong key)");
        let payload = Payload {
            msg: ciphertext,
            aad,
        };
        match self {
            Cipher::Xchacha(cipher) => {
                let nonce: &[u8; XCHACHA_NONCE_LEN] = nonce.try_into().map_err(|_| failed())?;
                cipher.decrypt(nonce.into(), payload)
            }
            Cipher::Aes(cipher) => {
                let nonce: &[u8; AES_GCM_NONCE_LEN] = nonce.try_into().map_err(|_| failed())?;
                cipher.decrypt(nonce.into(), payload)
            }
        }
        .map_err(|_| failed())
    }
}

/// A ready-to-use AEAD engine built from an [`EncryptionConfig`]: the active
/// seal key plus any extra decrypt-only keys (rotation).
pub struct Crypto {
    cipher: CipherKind,
    key_id: String,
    key: [u8; 32],
    active: Cipher,
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
            active: Cipher::new(config.cipher, &key),
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

        let mut out =
            Vec::with_capacity(3 + self.key_id.len() + nonce_len + plaintext.len() + TAG_LEN);
        out.push(ENVELOPE_VERSION);
        out.push(cipher_byte);
        out.push(self.key_id.len() as u8);
        out.extend_from_slice(self.key_id.as_bytes());
        out.extend_from_slice(nonce);
        let body = out.len();
        out.extend_from_slice(plaintext);
        self.active.seal_in_place(nonce, aad, &mut out, body)?;
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
        let (kind, nonce_len) = match cipher_byte {
            CIPHER_XCHACHA => (CipherKind::Xchacha20poly1305, XCHACHA_NONCE_LEN),
            CIPHER_AES_GCM => (CipherKind::Aes256gcm, AES_GCM_NONCE_LEN),
            other => return Err(anyhow!("unknown encryption cipher id {other}")),
        };
        if rest.len() < nonce_len {
            return Err(err());
        }
        let (nonce, ciphertext) = rest.split_at(nonce_len);

        // Anything sealed by this config opens with the pre-built cipher; only a
        // rotation key or a foreign cipher needs one built here.
        let rotated;
        let cipher = if kind == self.cipher && key_id == self.key_id {
            &self.active
        } else {
            let key = if key_id == self.key_id {
                &self.key
            } else {
                self.decrypt_keys
                    .get(key_id)
                    .ok_or_else(|| anyhow!("no decryption key for key_id '{key_id}'"))?
            };
            rotated = Cipher::new(kind, key);
            &rotated
        };
        cipher.open(nonce, aad, ciphertext)
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

    /// Wire-format pin: envelopes sealed by the previous implementation (cipher built
    /// per call, ciphertext allocated and copied in) must still open unchanged.
    #[test]
    fn opens_envelopes_from_the_previous_seal() {
        const VECTORS: [(CipherKind, &str); 2] = [
            (
                CipherKind::Xchacha20poly1305,
                "0100026b31090909090909090909090909090909090909090909090909cd1781f819de5912956b645309178325d684b4b084007ae1f95d5e8bbdfa",
            ),
            (
                CipherKind::Aes256gcm,
                "0101026b3109090909090909090909090954e0e7e6db84e111c11ba757878ea2b887e0ce8ffe2dd4d315dcc7c6ae6d",
            ),
        ];
        for (cipher, hex) in VECTORS {
            let envelope: Vec<u8> = (0..hex.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
                .collect();
            let crypto = Crypto::new(&config(cipher)).unwrap();
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
