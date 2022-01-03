use std::time::Duration;

use crate::result::Result;
use foundationdb::api::{FdbApiBuilder, NetworkAutoStop};
use foundationdb::{Database, Transaction};
use tokio::time::timeout;

pub struct FdbClient {
    pub db: Database,
}

impl FdbClient {
    /// # Safety
    /// This function is unsafe because it starts a background thread using the C API of fdb.
    pub unsafe fn start_network() -> Result<NetworkAutoStop> {
        let network_builder = FdbApiBuilder::default().build()?;

        network_builder.boot().map_err(Into::into)
    }

    pub fn new(path: &str) -> Result<Self> {
        let db = Database::new(Some(path))?;
        Ok(Self { db })
    }

    pub async fn begin_tx(&self) -> Result<Transaction> {
        let f = async { self.db.create_trx().map_err(Into::into) };

        match timeout(Duration::from_secs(1), f).await {
            Ok(tx) => tx,
            Err(e) => Err(e.into()),
        }
    }
}
