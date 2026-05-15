use crate::errors::PublisherError;
use crate::traits::{BatchCommitFunc, CommitFunc};
use crate::CanonicalMessage;

/// The outcome of a successful command handling operation.
#[derive(Debug)]
pub enum Handled {
    /// The command was handled successfully. No further message should be sent.
    /// This is equivalent to acknowledging the message.
    Ack,
    /// The command was handled successfully and produced a response to be published.
    Publish(CanonicalMessage),
}

/// The outcome of a successful single message publishing operation.
#[derive(Debug)]
pub enum Sent {
    /// Message was successfully sent, no response was generated.
    Ack,
    /// Message was successfully sent and a response was generated.
    Response(CanonicalMessage),
}

/// The outcome of a successful batch message publishing operation.
#[derive(Debug)]
pub enum SentBatch {
    /// All messages in the batch were sent successfully. No responses were generated.
    Ack,
    /// The batch operation resulted in a mix of successes and/or failures.
    Partial {
        responses: Option<Vec<CanonicalMessage>>,
        failed: Vec<(CanonicalMessage, PublisherError)>,
    },
}

/// A successfully received single message.
pub struct Received {
    pub message: CanonicalMessage,
    pub commit: CommitFunc,
}

impl std::fmt::Debug for Received {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Received")
            .field("message", &self.message)
            .field("commit", &"<CommitFunc>")
            .finish()
    }
}

/// A successfully received batch of messages.
pub struct ReceivedBatch {
    pub messages: Vec<CanonicalMessage>,
    pub commit: BatchCommitFunc,
}

impl std::fmt::Debug for ReceivedBatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReceivedBatch")
            .field("messages", &self.messages)
            .field("commit", &"<BatchCommitFunc>")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::MessageDisposition;

    #[test]
    fn test_received_debug_hides_commit_implementation() {
        let received = Received {
            message: CanonicalMessage::from("single"),
            commit: Box::new(|_| Box::pin(async move { Ok(()) })),
        };

        let debug = format!("{received:?}");
        assert!(debug.contains("Received"));
        assert!(debug.contains("<CommitFunc>"));
        assert!(debug.contains("single"));
    }

    #[test]
    fn test_received_batch_debug_hides_commit_implementation() {
        let batch = ReceivedBatch {
            messages: vec![
                CanonicalMessage::from("first"),
                CanonicalMessage::from("second"),
            ],
            commit: Box::new(|dispositions| {
                Box::pin(async move {
                    assert_eq!(dispositions.len(), 2);
                    assert!(dispositions
                        .into_iter()
                        .all(|disposition| matches!(disposition, MessageDisposition::Ack)));
                    Ok(())
                })
            }),
        };

        let debug = format!("{batch:?}");
        assert!(debug.contains("ReceivedBatch"));
        assert!(debug.contains("<BatchCommitFunc>"));
        assert!(debug.contains("first"));
        assert!(debug.contains("second"));
    }
}
