use thiserror::Error;

/// Errors that can occur during message processing (handling or publishing).
#[derive(Error, Debug)]
pub enum ProcessingError {
    /// A transient error occurred. The operation should be retried.
    #[error("retryable error: {0}")]
    Retryable(#[source] anyhow::Error),
    /// A permanent error occurred. The operation should not be retried.
    #[error("non-retryable error: {0}")]
    NonRetryable(#[source] anyhow::Error),
    /// A connection-level error occurred. Used to signal broker disconnects, etc.
    #[error("connection error: {0}")]
    Connection(#[source] anyhow::Error),
}
impl ProcessingError {
    pub fn is_connection_error(&self) -> bool {
        matches!(self, ProcessingError::Connection(_))
    }
}

pub type HandlerError = ProcessingError;
pub type PublisherError = ProcessingError;

/// Errors that can occur when consuming messages.
#[derive(Error, Debug)]
pub enum ConsumerError {
    /// A transport-level or other error occurred that should trigger a reconnect.
    #[error("consumer connection error: {0}")]
    Connection(#[source] anyhow::Error),

    /// A consumer gap was detected: the requested events were already garbage-collected.
    #[error("consumer gap: requested offset {requested} but earliest available is {base}")]
    Gap { requested: u64, base: u64 },

    /// The consumer has reached the end of the stream and has shut down gracefully.
    #[error("consumer reached end of stream")]
    EndOfStream,

    /// A permanent, non-retryable error: the message cannot be processed and must
    /// not be re-read (e.g. failed AEAD decryption/authentication of a payload).
    #[error("permanent consumer error: {0}")]
    Permanent(#[source] anyhow::Error),
}

impl From<anyhow::Error> for ConsumerError {
    fn from(err: anyhow::Error) -> Self {
        // By default, we'll treat any generic error as a connection-level, retryable error.
        ConsumerError::Connection(err)
    }
}

impl From<anyhow::Error> for ProcessingError {
    fn from(err: anyhow::Error) -> Self {
        // Default to Retryable for generic errors. Callers should use
        // ProcessingError::NonRetryable directly for known permanent failures.
        ProcessingError::Retryable(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_connection_error_only_matches_connection_variant() {
        assert!(ProcessingError::Connection(anyhow::anyhow!("offline")).is_connection_error());
        assert!(!ProcessingError::Retryable(anyhow::anyhow!("retry")).is_connection_error());
        assert!(!ProcessingError::NonRetryable(anyhow::anyhow!("stop")).is_connection_error());
    }

    #[test]
    fn test_anyhow_error_conversions_use_default_variants() {
        let consumer_error = ConsumerError::from(anyhow::anyhow!("consumer failure"));
        assert!(matches!(consumer_error, ConsumerError::Connection(_)));

        let processing_error = ProcessingError::from(anyhow::anyhow!("processing failure"));
        assert!(matches!(processing_error, ProcessingError::Retryable(_)));
    }

    #[test]
    fn test_consumer_error_display_messages() {
        assert_eq!(
            ConsumerError::Gap {
                requested: 42,
                base: 9
            }
            .to_string(),
            "consumer gap: requested offset 42 but earliest available is 9"
        );
        assert_eq!(
            ConsumerError::EndOfStream.to_string(),
            "consumer reached end of stream"
        );
    }
}
