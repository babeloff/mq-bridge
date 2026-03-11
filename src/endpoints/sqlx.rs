//  mq-bridge
//  © Copyright 2026, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/mq-bridge

use crate::canonical_message::tracing_support::LazyMessageIds;
use crate::models::SqlxConfig;
use crate::traits::{
    BoxFuture, ConsumerError, MessageConsumer, MessageDisposition, MessagePublisher,
    PublisherError, ReceivedBatch, Sent, SentBatch,
};
use crate::CanonicalMessage;
use anyhow::{anyhow, Context};
use async_trait::async_trait;
use sqlx::{AnyPool, Row};
use std::any::Any;
use std::time::Duration;
use tracing::trace;

pub struct SqlxPublisher {
    pool: AnyPool,
    insert_query: String,
}

impl SqlxPublisher {
    pub async fn new(config: &SqlxConfig) -> anyhow::Result<Self> {
        let pool = AnyPool::connect(&config.url).await?;
        let insert_query = config
            .insert_query
            .clone()
            .unwrap_or_else(|| format!("INSERT INTO {} (payload) VALUES (?)", config.table));
        Ok(Self { pool, insert_query })
    }
}

#[async_trait]
impl MessagePublisher for SqlxPublisher {
    async fn send(&self, message: CanonicalMessage) -> Result<Sent, PublisherError> {
        sqlx::query(&self.insert_query)
            .bind(message.payload.to_vec())
            .execute(&self.pool)
            .await
            .map_err(|e| PublisherError::Retryable(anyhow!(e)))?;
        Ok(Sent::Ack)
    }

    async fn send_batch(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> Result<SentBatch, PublisherError> {
        trace!(count = messages.len(), message_ids = ?LazyMessageIds(&messages), "Publishing batch to SQLx");
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| PublisherError::Retryable(anyhow!(e)))?;

        for msg in messages {
            sqlx::query(&self.insert_query)
                .bind(msg.payload.to_vec())
                .execute(&mut *tx)
                .await
                .map_err(|e| PublisherError::Retryable(anyhow!(e)))?;
        }

        tx.commit()
            .await
            .map_err(|e| PublisherError::Retryable(anyhow!(e)))?;

        Ok(SentBatch::Ack)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub struct SqlxConsumer {
    pool: AnyPool,
    select_query: String,
    delete_after_read: bool,
    table: String,
    polling_interval: Duration,
}

impl SqlxConsumer {
    pub async fn new(config: &SqlxConfig) -> anyhow::Result<Self> {
        let pool = AnyPool::connect(&config.url).await?;
        let select_query = config.select_query.clone().unwrap_or_else(|| {
            // A basic select query that might need adjustment for different SQL dialects
            // regarding locking and fetching. This is a reasonable default.
            format!("SELECT id, payload FROM {} LIMIT 100", config.table)
        });
        Ok(Self {
            pool,
            select_query,
            delete_after_read: config.delete_after_read,
            table: config.table.clone(),
            polling_interval: Duration::from_millis(config.polling_interval_ms.unwrap_or(100)),
        })
    }
}

#[async_trait]
impl MessageConsumer for SqlxConsumer {
    async fn receive_batch(&mut self, max_messages: usize) -> Result<ReceivedBatch, ConsumerError> {
        loop {
            let rows = sqlx::query(&self.select_query)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| ConsumerError::Connection(anyhow!(e)))?;

            if !rows.is_empty() {
                let mut messages = Vec::new();
                let mut ids_to_delete = Vec::new();

                for row in rows.into_iter().take(max_messages) {
                    let payload: Vec<u8> = row
                        .try_get("payload")
                        .context("Failed to get 'payload' column")?;
                    let id: i64 = row.try_get("id").context("Failed to get 'id' column")?;
                    messages.push(CanonicalMessage::new(payload, None));
                    ids_to_delete.push(id);
                }

                let pool = self.pool.clone();
                let table = self.table.clone();
                let delete = self.delete_after_read;

                let commit = Box::new(move |dispositions: Vec<MessageDisposition>| {
                    let pool = pool.clone();
                    let table = table.clone();
                    let ids = ids_to_delete.clone();
                    Box::pin(async move {
                        if !delete {
                            return Ok(());
                        }
                        let mut ids_to_ack = Vec::new();
                        for (i, disp) in dispositions.iter().enumerate() {
                            if matches!(
                                disp,
                                MessageDisposition::Ack | MessageDisposition::Reply(_)
                            ) {
                                if let Some(id) = ids.get(i) {
                                    ids_to_ack.push(*id);
                                }
                            }
                        }

                        if !ids_to_ack.is_empty() {
                            // This query works for postgres, but might need adjustment for mysql/sqlite
                            let placeholders = ids_to_ack
                                .iter()
                                .map(|_| "?")
                                .collect::<Vec<_>>()
                                .join(", ");
                            let query_str =
                                format!("DELETE FROM {} WHERE id IN ({})", table, placeholders);
                            let mut query = sqlx::query(&query_str);
                            for id in ids_to_ack {
                                query = query.bind(id);
                            }
                            query
                                .execute(&pool)
                                .await
                                .map_err(|e| anyhow!("Failed to delete acked messages: {}", e))?;
                        }
                        Ok(())
                    }) as BoxFuture<'static, anyhow::Result<()>>
                });

                return Ok(ReceivedBatch { messages, commit });
            }

            tokio::time::sleep(self.polling_interval).await;
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
