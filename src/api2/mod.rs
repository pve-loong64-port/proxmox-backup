//! The Proxmox Backup Server API

use std::future::Future;

use proxmox_sortable_macro::sortable;

pub mod access;
pub mod admin;
pub mod backup;
pub mod config;
pub mod helpers;
pub mod node;
pub mod ping;
pub mod pull;
pub mod reader;
pub mod status;
pub mod tape;
pub mod types;
pub mod version;

use proxmox_router::{list_subdirs_api_method, Router, SubdirMap};

#[sortable]
const SUBDIRS: SubdirMap = &sorted!([
    ("access", &access::ROUTER),
    ("admin", &admin::ROUTER),
    ("backup", &backup::ROUTER),
    ("config", &config::ROUTER),
    ("nodes", &node::ROUTER),
    ("ping", &ping::ROUTER),
    ("pull", &pull::ROUTER),
    ("reader", &reader::ROUTER),
    ("status", &status::ROUTER),
    ("tape", &tape::ROUTER),
    ("version", &version::ROUTER),
]);

pub const ROUTER: Router = Router::new()
    .get(&list_subdirs_api_method!(SUBDIRS))
    .subdirs(SUBDIRS);

#[derive(Clone)]
struct ExecInheritLogContext;

impl<Fut> hyper::rt::Executor<Fut> for ExecInheritLogContext
where
    Fut: Future + Send + 'static,
    Fut::Output: Send,
{
    fn execute(&self, fut: Fut) {
        use proxmox_log::LogContext;

        match LogContext::current() {
            None => tokio::spawn(fut),
            Some(context) => tokio::spawn(context.scope(fut)),
        };
    }
}
