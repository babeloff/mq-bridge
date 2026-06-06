//  mq-bridge
//  © Copyright 2025, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/mq-bridge

#![cfg(windows)]

use super::transport::TransportChannel;
use crate::CanonicalMessage;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::windows::named_pipe::{
    ClientOptions, NamedPipeClient, NamedPipeServer, ServerOptions,
};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

/// Windows Named Pipe transport for local IPC
#[derive(Clone)]
pub struct WindowsIpcTransport {
    inner: Arc<WindowsIpcTransportInner>,
}

struct WindowsIpcTransportInner {
    pipe_name: String,
    capacity: usize,
    // For server mode (consumer)
    server: Mutex<Option<NamedPipeServer>>,
    // For client mode (publisher)
    client: Mutex<Option<NamedPipeClient>>,
    connected: Mutex<bool>,
    closed: Mutex<bool>,
}

impl WindowsIpcTransport {
    /// Create a new Windows Named Pipe transport as a server (consumer side)
    pub async fn new_server(pipe_name: impl AsRef<str>, capacity: usize) -> Result<Self> {
        let pipe_name = pipe_name.as_ref();
        let full_path = format!(r"\\.\pipe\{}", pipe_name);

        let server = ServerOptions::new()
            .first_pipe_instance(true)
            .create(&full_path)?;

        info!(pipe = %full_path, "Windows Named Pipe server created");

        Ok(Self {
            inner: Arc::new(WindowsIpcTransportInner {
                pipe_name: full_path,
                capacity,
                server: Mutex::new(Some(server)),
                client: Mutex::new(None),
                connected: Mutex::new(false),
                closed: Mutex::new(false),
            }),
        })
    }

    /// Create a new Windows Named Pipe transport as a client (publisher side)
    pub async fn new_client(pipe_name: impl AsRef<str>, capacity: usize) -> Result<Self> {
        let pipe_name = pipe_name.as_ref();
        let full_path = format!(r"\\.\pipe\{}", pipe_name);

        let client = ClientOptions::new().open(&full_path)?;

        info!(pipe = %full_path, "Windows Named Pipe client connected");

        Ok(Self {
            inner: Arc::new(WindowsIpcTransportInner {
                pipe_name: full_path,
                capacity,
                server: Mutex::new(None),
                client: Mutex::new(Some(client)),
                connected: Mutex::new(true),
                closed: Mutex::new(false),
            }),
        })
    }

    /// Send a length-prefixed frame
    async fn send_frame<W>(pipe: &mut W, data: &[u8]) -> Result<()>
    where
        W: AsyncWrite + Unpin + Send,
    {
        let len = data.len() as u32;
        pipe.write_all(&len.to_be_bytes()).await?;
        pipe.write_all(data).await?;
        pipe.flush().await?;
        Ok(())
    }

    /// Receive a length-prefixed frame
    async fn recv_frame<R>(pipe: &mut R) -> Result<Vec<u8>>
    where
        R: AsyncRead + Unpin + Send,
    {
        let mut len_bytes = [0u8; 4];
        pipe.read_exact(&mut len_bytes).await?;
        let len = u32::from_be_bytes(len_bytes) as usize;

        // Sanity check: limit frame size to 100MB
        if len > 100 * 1024 * 1024 {
            return Err(anyhow!("Frame too large: {} bytes", len));
        }

        let mut data = vec![0u8; len];
        pipe.read_exact(&mut data).await?;
        Ok(data)
    }

    /// Wait for a client connection (server mode)
    pub async fn wait_for_connection(&self) -> Result<()> {
        if *self.inner.connected.lock().await {
            return Ok(());
        }

        let mut server_guard = self.inner.server.lock().await;
        if let Some(server) = server_guard.as_mut() {
            server.connect().await?;
            *self.inner.connected.lock().await = true;
            debug!(pipe = %self.inner.pipe_name, "Client connected to Named Pipe");
            Ok(())
        } else {
            Err(anyhow!("Windows Named Pipe transport not in server mode"))
        }
    }
}

#[async_trait]
impl TransportChannel for WindowsIpcTransport {
    async fn send_batch(&self, messages: Vec<CanonicalMessage>) -> Result<()> {
        if *self.inner.closed.lock().await {
            return Err(anyhow!("Windows Named Pipe transport is closed"));
        }

        // Serialize the batch using MessagePack
        let data = rmp_serde::to_vec(&messages)?;

        let mut client_guard = self.inner.client.lock().await;
        if let Some(client) = client_guard.as_mut() {
            Self::send_frame(client, &data).await?;
            debug!(
                pipe = %self.inner.pipe_name,
                count = messages.len(),
                bytes = data.len(),
                "Sent batch via Named Pipe"
            );
            Ok(())
        } else {
            Err(anyhow!("Windows Named Pipe transport not in client mode"))
        }
    }

    async fn recv_batch(&self) -> Result<Vec<CanonicalMessage>> {
        if *self.inner.closed.lock().await {
            return Err(anyhow!("Windows Named Pipe transport is closed"));
        }

        // Wait for connection if we haven't connected yet (server mode)
        self.wait_for_connection().await?;
        let mut server_guard = self.inner.server.lock().await;
        if let Some(server) = server_guard.as_mut() {
            let data = Self::recv_frame(server).await?;
            let messages: Vec<CanonicalMessage> = rmp_serde::from_slice(&data)?;
            debug!(
                pipe = %self.inner.pipe_name,
                count = messages.len(),
                bytes = data.len(),
                "Received batch via Named Pipe"
            );
            Ok(messages)
        } else {
            Err(anyhow!("Windows Named Pipe transport not in server mode"))
        }
    }

    fn try_recv_batch(&self) -> Result<Option<Vec<CanonicalMessage>>> {
        // Named pipes don't support non-blocking try_recv in the same way
        // This would require a more complex implementation with polling
        // For now, return None to indicate no immediate data available
        Ok(None)
    }

    fn len(&self) -> usize {
        // Named pipes don't have a concept of queued messages
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
            info!(pipe = %self.inner.pipe_name, "Closing Windows Named Pipe transport");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_windows_pipe_roundtrip() {
        let pipe_name = format!("mq-bridge-test-{}", uuid::Uuid::new_v4());

        // Create server
        let server = WindowsIpcTransport::new_server(&pipe_name, 10)
            .await
            .unwrap();

        // Create client in a separate task
        let pipe_name_clone = pipe_name.clone();
        let client_task = tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            WindowsIpcTransport::new_client(&pipe_name_clone, 10)
                .await
                .unwrap()
        });

        // Wait for connection
        server.wait_for_connection().await.unwrap();

        let client = client_task.await.unwrap();

        // Send from client
        let msg = CanonicalMessage::default();
        client.send_batch(vec![msg.clone()]).await.unwrap();

        // Receive on server
        let received = server.recv_batch().await.unwrap();
        assert_eq!(received.len(), 1);
    }

    #[tokio::test]
    async fn test_windows_pipe_close() {
        let pipe_name = format!("mq-bridge-test-{}", uuid::Uuid::new_v4());

        let server = WindowsIpcTransport::new_server(&pipe_name, 10)
            .await
            .unwrap();

        assert!(!server.is_closed());
        server.close();
        assert!(server.is_closed());
    }
}

// Made with Bob
