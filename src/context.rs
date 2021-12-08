use std::collections::HashMap;
use std::sync::Arc;

use crate::clickhouse_message_binding::MessageBinding;
use crate::fdb::FdbClient;
use crate::clickhouse::Client as ClickhouseClient;

pub type Registry<'a> = HashMap<String, MessageBinding<'a>>;

pub struct AppContext<'a> {
    pub fdb_client: Arc<FdbClient>,
    pub ch_client: ClickhouseClient,
    pub proto_registry: Registry<'a>,
}

impl<'a> AppContext<'a> {
    pub fn new(
        fdb_client: Arc<FdbClient>,
        ch_client: ClickhouseClient,
    ) -> AppContext<'a> {
        AppContext {
            fdb_client,
            ch_client,
            proto_registry: HashMap::new()
        }
    }

    pub fn to_string(&self) {
        for (key, value) in &self.proto_registry {
            println!("{}: {}", key, value.table.parts.to_string());
        }
    }
}