use std::collections::HashMap;

use crate::clickhouse_table::MessageBinding;
use crate::fdb::FdbClient;
use crate::clickhouse::Client as ClickhouseClient;

pub type Registry = HashMap<String, MessageBinding>;

pub struct AppContext {
    pub fdb_client: FdbClient,
    pub ch_client: ClickhouseClient,
    pub proto_registry: Registry,
}

impl AppContext {
    pub fn new(
        fdb_client: FdbClient,
        ch_client: ClickhouseClient,
    ) -> AppContext {
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