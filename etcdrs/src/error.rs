use std::fmt;

pub struct Error {
    inner: ErrorInner,
}

impl Error {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            inner: ErrorInner {
                kind,
                content: ErrorContent::Dynamic(message.into()),
            },
        }
    }

    pub fn kind(&self) -> ErrorKind {
        self.inner.kind
    }
}

impl From<ErrorKind> for Error {
    fn from(value: ErrorKind) -> Self {
        ErrorInner::with_static_message(value, value.default_message()).into()
    }
}

impl From<ErrorInner> for Error {
    fn from(value: ErrorInner) -> Self {
        Self { inner: value }
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Error")
            .field("kind", &self.kind())
            .field("content", &self.inner.content)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum ErrorKind {
    Unknown,
    Canceled,
    /// Specified arguments were invalid. The associated message might have more details about which arguments were
    /// invalid.
    InvalidArgument,
    /// The system is currently unavailable.
    Unavailable,
    /// The key was not found.
    NotFound,
    /// Too many items were returned.
    TooMany,
}

impl ErrorKind {
    fn default_message(&self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Canceled => "canceled",
            Self::InvalidArgument => "invalid argument",
            Self::Unavailable => "unavailable",
            Self::NotFound => "not found",
            Self::TooMany => "too many items",
        }
    }
}

pub(crate) struct ErrorInner {
    kind: ErrorKind,
    content: ErrorContent,
}

impl ErrorInner {
    /// Create an error with a static error code.
    pub fn with_static_message(kind: ErrorKind, message: &'static str) -> Self {
        Self {
            kind,
            content: ErrorContent::Static(message),
        }
    }

    pub fn from_unknown(other: impl std::error::Error + 'static) -> Self {
        Self {
            kind: ErrorKind::Unknown,
            content: ErrorContent::Other(Box::new(other)),
        }
    }
}

enum ErrorContent {
    Static(&'static str),
    Dynamic(String),
    Other(Box<dyn std::error::Error>),
}

impl fmt::Debug for ErrorContent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Static(msg) => write!(f, "{msg:?}"),
            Self::Dynamic(msg) => write!(f, "{msg:?}"),
            Self::Other(err) => write!(f, "{err:?}"),
        }
    }
}
