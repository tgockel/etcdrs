// Declaration order here is the order the labeled `impl Client` sections appear on the `Client`
// page; rustdoc lists inherent impls in the order it finds them. Grouped by concern rather than
// alphabetized for that reason.
mod get;
pub use get::*;

mod list;
pub(crate) use list::ListContinuation;
pub use list::*;

mod put;
pub use put::*;

mod delete;
pub use delete::*;

mod transaction;
pub use transaction::*;

mod watch;
pub use watch::*;

mod lease;
pub use lease::*;

mod compact;
pub use compact::*;

mod auth;
pub use auth::*;

mod user;
pub use user::*;

mod role;
pub use role::*;

mod cluster;
pub use cluster::*;
