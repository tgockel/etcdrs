//! Creating a client from a thread that is not running a Tokio runtime.

use etcdrs::Client;
use rstest::rstest;

/// One endpoint makes the channel with `connect_lazy` and several with `balance_list`; both spawn a task.
#[rstest]
#[case::one_endpoint("http://127.0.0.1:2379")]
#[case::several_endpoints("http://127.0.0.1:2379,http://127.0.0.1:2380")]
#[should_panic(expected = "there is no reactor running")]
fn new_outside_a_runtime_panics(#[case] connect_string: &str) {
    let _ = Client::new(connect_string);
}
