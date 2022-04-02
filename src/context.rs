use std::collections::HashMap;
use std::sync::Arc;

// use crate::clickhouse::Client as ClickhouseClient;
use crate::{postgres_message_binding::MessageBinding, postgres};
use crate::fdb::FdbClient;
use tracing::*;

pub type Registry<'a> = HashMap<String, MessageBinding<'a>>;

pub struct AppContext<'a> {
    pub fdb_client: Arc<FdbClient>,
    pub pg_client: postgres::Client,
    pub proto_registry: Registry<'a>,
}

impl<'a> AppContext<'a> {
    pub fn new(fdb_client: Arc<FdbClient>, pg_client: postgres::Client) -> AppContext<'a> {
        AppContext {
            fdb_client,
            pg_client,
            proto_registry: HashMap::new(),
        }
    }

    pub fn to_string(&self) {
        for (key, value) in &self.proto_registry {
            info!("{}: {}", key, value.table.parts.to_string());
        }
    }
}
