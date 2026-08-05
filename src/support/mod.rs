//  mq-bridge
//  © Copyright 2026, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/mq-bridge

//! Cross-cutting support utilities used across endpoints and middleware:
//! cryptographic primitives, payload (de)compression, `${...}` string
//! interpolation, and the shared connection registry.

pub mod base64_engine;
#[cfg(feature = "compression")]
pub(crate) mod compression;
pub mod connection_registry;
#[cfg(feature = "encryption")]
pub mod crypto;
pub(crate) mod crypto_envelope;
pub mod interpolation;
