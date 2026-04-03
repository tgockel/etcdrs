use std::backtrace::Backtrace;
use std::borrow::Cow;

/// A trait implemented by all operation-specific error types.
///
/// This enables generic code that works across different operation errors — for example, health
/// checks or logging that don't need to know the specific operation.
pub trait OperationError: std::error::Error {
    /// The original gRPC status, if this error originated from a gRPC call.
    fn grpc_status(&self) -> Option<&tonic::Status>;

    /// The backtrace captured when the error was created.
    fn backtrace(&self) -> &Backtrace;
}

/// Internal error representation shared by all operation-specific error types.
pub(crate) struct ErrorRepr<K> {
    pub(crate) kind: K,
    pub(crate) message: Cow<'static, str>,
    pub(crate) status: Option<tonic::Status>,
    pub(crate) backtrace: Backtrace,
}

impl<K> ErrorRepr<K> {
    /// Returns the best available message: the explicit message if non-empty, otherwise the gRPC
    /// status message. This avoids duplicating `status.message()` into the `message` field.
    pub(crate) fn display_message(&self) -> &str {
        if !self.message.is_empty() {
            &self.message
        } else if let Some(status) = &self.status {
            status.message()
        } else {
            ""
        }
    }
}

/// Generates an operation error struct with standard methods and trait impls.
///
/// The generated struct wraps `Box<ErrorRepr<$kind>>` and provides:
/// - `kind()`, `grpc_status()`, `take_grpc_status()`, `backtrace()`
/// - `Debug`, `Display`, `std::error::Error`, and `OperationError` impls.
///
/// Each call site is expected to define the kind enum separately and implement
/// `from_status(tonic::Status) -> Self` with operation-specific code mapping.
macro_rules! define_op_error {
    (
        $(#[$meta:meta])*
        $vis:vis struct $name:ident($kind:ty);
    ) => {
        $(#[$meta])*
        $vis struct $name(Box<$crate::error::ErrorRepr<$kind>>);

        impl $name {
            pub(crate) fn new(
                kind: $kind,
                message: impl Into<std::borrow::Cow<'static, str>>,
                status: Option<tonic::Status>,
            ) -> Self {
                Self(Box::new($crate::error::ErrorRepr {
                    kind,
                    message: message.into(),
                    status,
                    backtrace: std::backtrace::Backtrace::capture(),
                }))
            }

            /// The operation-specific error kind.
            pub fn kind(&self) -> $kind {
                self.0.kind
            }

            /// The original gRPC status, if this error originated from a gRPC call.
            pub fn grpc_status(&self) -> Option<&tonic::Status> {
                self.0.status.as_ref()
            }

            /// Take ownership of the original gRPC status, if present.
            pub fn take_grpc_status(self) -> Option<tonic::Status> {
                self.0.status
            }

            /// The backtrace captured when this error was created.
            pub fn backtrace(&self) -> &std::backtrace::Backtrace {
                &self.0.backtrace
            }
        }

        impl std::fmt::Debug for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct(stringify!($name))
                    .field("kind", &self.0.kind)
                    .field("message", &self.0.display_message())
                    .finish()
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{:?}: {}", self.0.kind, self.0.display_message())
            }
        }

        impl std::error::Error for $name {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                self.0.status.as_ref().map(|s| s as _)
            }
        }

        impl $crate::error::OperationError for $name {
            fn grpc_status(&self) -> Option<&tonic::Status> {
                self.0.status.as_ref()
            }

            fn backtrace(&self) -> &std::backtrace::Backtrace {
                &self.0.backtrace
            }
        }
    };
}
