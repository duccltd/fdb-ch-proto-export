use std::collections::HashMap;
use std::time::Duration;

use foundationdb::api::{FdbApiBuilder, NetworkAutoStop};
use foundationdb::{Database, RangeOption, Transaction};
use tokio::time::timeout;
use tracing::*;
use crate::result::Result;

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

    pub async fn get<'a>(&self, tx: &'a Transaction, key: &'a [u8]) -> Result<Option<Vec<u8>>> {
        debug!("get {}", String::from_utf8_lossy(key));
        let opt_val = tx.get(key, false).await?;
        let val = match opt_val {
            None => return Ok(None),
            Some(val) => val,
        };
        Ok(Some((*val).to_vec()))
    }

    pub async fn get_range<'a>(
        &self,
        tx: &'a Transaction,
        from: &'a [u8],
        to: &'a [u8],
    ) -> Result<HashMap<Vec<u8>, Vec<u8>>> {
        debug!(
            "get_range {} {}",
            String::from_utf8_lossy(from),
            String::from_utf8_lossy(to),
        );

        let values = tx
            .get_range(
                &RangeOption {
                    reverse: false,
                    limit: None,
                    mode: foundationdb::options::StreamingMode::WantAll,
                    ..RangeOption::from((from, to))
                },
                1,
                false,
            )
            .await?;

        // Ok(values.into_iter().map(|kv| kv.key().to_vec()).collect())
        let mut map: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        values.into_iter().for_each(|kv| {
            map.insert(kv.key().to_vec(), kv.value().to_vec());
        });

        Ok(map)
    }
}