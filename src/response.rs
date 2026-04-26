use crate::traits::{Handled, HandlerError};
use crate::CanonicalMessage;

/// Internal trait to convert success types into the [Handled] enum.
pub trait IntoHandled {
    fn into_handled(self) -> Handled;
}

impl IntoHandled for Handled {
    fn into_handled(self) -> Handled {
        self
    }
}

impl IntoHandled for () {
    fn into_handled(self) -> Handled {
        Handled::Ack
    }
}

impl IntoHandled for CanonicalMessage {
    fn into_handled(self) -> Handled {
        Handled::Publish(self)
    }
}

impl IntoHandled for Option<CanonicalMessage> {
    fn into_handled(self) -> Handled {
        match self {
            Some(msg) => Handled::Publish(msg),
            None => Handled::Ack,
        }
    }
}

/// A trait for types that can be converted into a `Result<Handled, HandlerError>`.
///
/// This allows handlers to return ergonomic types instead of the explicit `Handled` enum.
///
/// ### Supported Success Types:
/// - `()`: Equivalent to `Handled::Ack` (Processed successfully).
/// - `CanonicalMessage`: Equivalent to `Handled::Publish(msg)` (Send a response).
/// - `Option<CanonicalMessage>`: `Some` sends a response, `None` acknowledges (Ack).
/// - `Handled`: The original enum (for full control).
///
/// ### Supported Error Types (via `ToHandlerError`):
/// - `anyhow::Error`: Treated as `Retryable` by default.
/// - `String` / `&str`: Treated as `NonRetryable`.
/// - `HandlerError`: The internal error type for explicit control.
///
/// # Examples
///
/// ```rust
/// use mq_bridge::{CanonicalMessage, Handled};
///
/// struct MyData { id: u32 }
///
/// async fn handle_reply(_: MyData) -> CanonicalMessage {
///     CanonicalMessage::from("Response")
/// }
///
/// // 2. Use anyhow::Result (Ok(()) is now inferred correctly in real logic)
/// async fn handle_fail(_: MyData) -> anyhow::Result<()> {
///     // Some logic that might use the '?' operator
///     if true { anyhow::bail!("Something went wrong"); }
///     
///     Ok(()) // No Handled::Ack needed
/// }
/// ```
pub trait IntoHandlerResult: Send + Sync + 'static {
    fn into_handler_result(self) -> Result<Handled, HandlerError>;
}

/// A marker for types that are considered 'ergonomic' (not the standard Result<Handled, HandlerError>).
/// This prevents type inference ambiguity in existing code.
pub trait ErgonomicResponse: IntoHandlerResult {}

impl IntoHandlerResult for Handled {
    fn into_handler_result(self) -> Result<Handled, HandlerError> {
        Ok(self)
    }
}
impl ErgonomicResponse for Handled {}

impl IntoHandlerResult for () {
    fn into_handler_result(self) -> Result<Handled, HandlerError> {
        Ok(Handled::Ack)
    }
}
impl ErgonomicResponse for () {}

impl IntoHandlerResult for CanonicalMessage {
    fn into_handler_result(self) -> Result<Handled, HandlerError> {
        Ok(Handled::Publish(self))
    }
}
impl ErgonomicResponse for CanonicalMessage {}

impl IntoHandlerResult for Option<CanonicalMessage> {
    fn into_handler_result(self) -> Result<Handled, HandlerError> {
        Ok(self.into_handled())
    }
}
impl ErgonomicResponse for Option<CanonicalMessage> {}

/// Explicit implementation for the standard result type.
/// Note: This is NOT an ErgonomicResponse to maintain disjoint implementations in IntoTypedHandler.
impl IntoHandlerResult for Result<Handled, HandlerError> {
    fn into_handler_result(self) -> Result<Handled, HandlerError> {
        self
    }
}

impl<E> IntoHandlerResult for Result<(), E>
where
    E: ToHandlerError + Send + Sync + 'static,
{
    fn into_handler_result(self) -> Result<Handled, HandlerError> {
        match self {
            Ok(_) => Ok(Handled::Ack),
            Err(err) => Err(err.to_handler_error()),
        }
    }
}
impl<E> ErgonomicResponse for Result<(), E> where E: ToHandlerError + Send + Sync + 'static {}

impl<E> IntoHandlerResult for Result<CanonicalMessage, E>
where
    E: ToHandlerError + Send + Sync + 'static,
{
    fn into_handler_result(self) -> Result<Handled, HandlerError> {
        match self {
            Ok(msg) => Ok(Handled::Publish(msg)),
            Err(err) => Err(err.to_handler_error()),
        }
    }
}
impl<E> ErgonomicResponse for Result<CanonicalMessage, E> where
    E: ToHandlerError + Send + Sync + 'static
{
}

impl<E> IntoHandlerResult for Result<Option<CanonicalMessage>, E>
where
    E: ToHandlerError + Send + Sync + 'static,
{
    fn into_handler_result(self) -> Result<Handled, HandlerError> {
        match self {
            Ok(val) => Ok(val.into_handled()),
            Err(err) => Err(err.to_handler_error()),
        }
    }
}
impl<E> ErgonomicResponse for Result<Option<CanonicalMessage>, E> where
    E: ToHandlerError + Send + Sync + 'static
{
}

/// Internal trait to convert error types into [HandlerError].
pub trait ToHandlerError {
    fn to_handler_error(self) -> HandlerError;
}

impl ToHandlerError for HandlerError {
    fn to_handler_error(self) -> HandlerError {
        self
    }
}

impl ToHandlerError for anyhow::Error {
    fn to_handler_error(self) -> HandlerError {
        crate::errors::ProcessingError::Retryable(self)
    }
}

impl ToHandlerError for String {
    fn to_handler_error(self) -> HandlerError {
        crate::errors::ProcessingError::NonRetryable(anyhow::anyhow!(self))
    }
}

impl ToHandlerError for &str {
    fn to_handler_error(self) -> HandlerError {
        crate::errors::ProcessingError::NonRetryable(anyhow::anyhow!(self.to_string()))
    }
}

#[cfg(test)]
mod tests {
    // Tests are integrated in type_handler or route tests, or added here
}
