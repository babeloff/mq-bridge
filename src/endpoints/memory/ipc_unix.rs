//  mq-bridge
//  © Copyright 2025, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/mq-bridge

#![cfg(unix)]

use super::transport::TransportChannel;
use crate::CanonicalMessage;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use std::path::Path;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

/// Unix Domain Socket transport for local IPC
#[derive(Clone)]
pub struct UnixIpcTransport {
    inner: Arc<UnixIpcTransportInner>,
}

struct UnixIpcTransportInner {
    socket_path: String,
    capacity: usize,
    // For server mode (consumer)
    listener: Mutex<Option<UnixListener>>,
    // For client mode (publisher)
    stream: Mutex<Option<UnixStream>>,
    closed: Mutex<bool>,
}

impl UnixIpcTransport {
    /// Create a new Unix IPC transport as a server (consumer side)
    pub async fn new_server(socket_path: impl AsRef<Path>, capacity: usize) -> Result<Self> {
        let socket_path = socket_path.as_ref();

        // Remove existing socket if it exists
        if socket_path.exists() {
            std::fs::remove_file(socket_path)?;
        }

        // Create parent directory if needed
        if let Some(parent) = socket_path.parent() {
            std::fs::create_dir_all(parent)?;
            // Set restrictive permissions on directory (0700)
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let mut perms = std::fs::metadata(parent)?.permissions();
                perms.set_mode(0o700);
                std::fs::set_permissions(parent, perms)?;
            }
        }

        let listener = UnixListener::bind(socket_path)?;

        // Set restrictive permissions on socket (0600)
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = std::fs::metadata(socket_path)?.permissions();
            perms.set_mode(0o600);
            std::fs::set_permissions(socket_path, perms)?;
        }

        info!(path = %socket_path.display(), "Unix IPC server listening");

        Ok(Self {
            inner: Arc::new(UnixIpcTransportInner {
                socket_path: socket_path.to_string_lossy().to_string(),
                capacity,
                listener: Mutex::new(Some(listener)),
                stream: Mutex::new(None),
                closed: Mutex::new(false),
            }),
        })
    }

    /// Create a new Unix IPC transport as a client (publisher side)
    pub async fn new_client(socket_path: impl AsRef<Path>, capacity: usize) -> Result<Self> {
        let socket_path = socket_path.as_ref();

        let stream = UnixStream::connect(socket_path).await?;

        info!(path = %socket_path.display(), "Unix IPC client connected");

        Ok(Self {
            inner: Arc::new(UnixIpcTransportInner {
                socket_path: socket_path.to_string_lossy().to_string(),
                capacity,
                listener: Mutex::new(None),
                stream: Mutex::new(Some(stream)),
                closed: Mutex::new(false),
            }),
        })
    }

    /// Send a length-prefixed frame
    async fn send_frame(stream: &mut UnixStream, data: &[u8]) -> Result<()> {
        let len = data.len() as u32;
        stream.write_all(&len.to_be_bytes()).await?;
        stream.write_all(data).await?;
        stream.flush().await?;
        Ok(())
    }

    /// Receive a length-prefixed frame
    async fn recv_frame(stream: &mut UnixStream) -> Result<Vec<u8>> {
        let mut len_bytes = [0u8; 4];
        stream.read_exact(&mut len_bytes).await?;
        let len = u32::from_be_bytes(len_bytes) as usize;

        // Sanity check: limit frame size to 100MB
        if len > 100 * 1024 * 1024 {
            return Err(anyhow!("Frame too large: {} bytes", len));
        }

        let mut data = vec![0u8; len];
        stream.read_exact(&mut data).await?;
        Ok(data)
    }

    /// Accept a connection (server mode)
    async fn accept_connection(&self) -> Result<UnixStream> {
        let listener_guard = self.inner.listener.lock().await;
        if let Some(listener) = listener_guard.as_ref() {
            let (stream, _addr) = listener.accept().await?;
            debug!(path = %self.inner.socket_path, "Accepted Unix IPC connection");
            Ok(stream)
        } else {
            Err(anyhow!("Unix IPC transport not in server mode"))
        }
    }

    fn is_disconnected(error: &anyhow::Error) -> bool {
        error
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io_error| {
                matches!(
                    io_error.kind(),
                    std::io::ErrorKind::UnexpectedEof
                        | std::io::ErrorKind::BrokenPipe
                        | std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::ConnectionAborted
                        | std::io::ErrorKind::NotConnected
                )
            })
    }
}

#[async_trait]
impl TransportChannel for UnixIpcTransport {
    async fn send_batch(&self, messages: Vec<CanonicalMessage>) -> Result<()> {
        if *self.inner.closed.lock().await {
            return Err(anyhow!("Unix IPC transport is closed"));
        }

        // Serialize the batch using MessagePack
        let data = rmp_serde::to_vec(&messages)?;

        let mut stream_guard = self.inner.stream.lock().await;
        if let Some(stream) = stream_guard.as_mut() {
            Self::send_frame(stream, &data).await?;
            debug!(
                path = %self.inner.socket_path,
                count = messages.len(),
                bytes = data.len(),
                "Sent batch via Unix IPC"
            );
            Ok(())
        } else {
            Err(anyhow!("Unix IPC transport not in client mode"))
        }
    }

    async fn recv_batch(&self) -> Result<Vec<CanonicalMessage>> {
        loop {
            if *self.inner.closed.lock().await {
                return Err(anyhow!("Unix IPC transport is closed"));
            }

            let mut stream_guard = self.inner.stream.lock().await;
            if stream_guard.is_none() {
                drop(stream_guard);
                let stream = self.accept_connection().await?;
                stream_guard = self.inner.stream.lock().await;
                *stream_guard = Some(stream);
            }

            let read_result = if let Some(stream) = stream_guard.as_mut() {
                Self::recv_frame(stream).await
            } else {
                Err(anyhow!("Unix IPC transport has no active connection"))
            };

            match read_result {
                Ok(data) => {
                    let messages: Vec<CanonicalMessage> = rmp_serde::from_slice(&data)?;
                    debug!(
                        path = %self.inner.socket_path,
                        count = messages.len(),
                        bytes = data.len(),
                        "Received batch via Unix IPC"
                    );
                    return Ok(messages);
                }
                Err(error) if Self::is_disconnected(&error) => {
                    warn!(path = %self.inner.socket_path, error = %error, "Unix IPC peer disconnected; waiting for a new connection");
                    *stream_guard = None;
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn try_recv_batch(&self) -> Result<Option<Vec<CanonicalMessage>>> {
        // Unix sockets don't support non-blocking try_recv in the same way
        // This would require a more complex implementation with polling
        // For now, return None to indicate no immediate data available
        Ok(None)
    }

    fn len(&self) -> usize {
        // Unix sockets don't have a concept of queued messages
        // Return 0 as we don't buffer
        0
    }

    fn capacity(&self) -> Option<usize> {
        Some(self.inner.capacity)
    }

    fn is_closed(&self) -> bool {
        // This is a blocking check, but should be fast
        if let Ok(closed) = self.inner.closed.try_lock() {
            *closed
        } else {
            false
        }
    }

    fn close(&self) {
        if let Ok(mut closed) = self.inner.closed.try_lock() {
            *closed = true;
            info!(path = %self.inner.socket_path, "Closing Unix IPC transport");
        }
    }
}

impl Drop for UnixIpcTransport {
    fn drop(&mut self) {
        // Clean up socket file if we're the server
        if let Ok(listener_guard) = self.inner.listener.try_lock() {
            if listener_guard.is_some() {
                let socket_path = &self.inner.socket_path;
                if let Err(e) = std::fs::remove_file(socket_path) {
                    if e.kind() != std::io::ErrorKind::NotFound {
                        warn!(path = %socket_path, error = %e, "Failed to remove Unix socket file");
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_unix_ipc_roundtrip() {
        let temp_dir = TempDir::new().unwrap();
        let socket_path = temp_dir.path().join("test.sock");

        // Create server
        let server = UnixIpcTransport::new_server(&socket_path, 10)
            .await
            .unwrap();

        // Create client
        let client = UnixIpcTransport::new_client(&socket_path, 10)
            .await
            .unwrap();

        // Send from client
        let msg = CanonicalMessage::from_vec(b"test");
        client.send_batch(vec![msg.clone()]).await.unwrap();

        // Receive on server
        let received = server.recv_batch().await.unwrap();
        assert_eq!(received.len(), 1);
    }

    #[tokio::test]
    async fn test_unix_ipc_close() {
        let temp_dir = TempDir::new().unwrap();
        let socket_path = temp_dir.path().join("test.sock");

        let server = UnixIpcTransport::new_server(&socket_path, 10)
            .await
            .unwrap();

        assert!(!server.is_closed());
        server.close();
        assert!(server.is_closed());
    }
}

// Made with Bob
