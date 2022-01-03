use crate::clickhouse_table::ClickhouseTableParts;
use crate::result::Result;
use serde::{Deserialize, Serialize};

#[derive(Debug, clickhouse::Row, Serialize, Deserialize, Clone)]
pub struct ClickhouseTableColumnRow {
    pub name: String,
    pub position: u64,
    pub r#type: String,
    pub default_expression: String,
}

pub struct Client {
    pub client: clickhouse::Client,
}

impl Client {
    pub fn new(client: clickhouse::Client) -> Self {
        Self { client }
    }

    pub async fn table_columns(
        &self,
        table: ClickhouseTableParts,
    ) -> Result<Vec<ClickhouseTableColumnRow>> {
        let rows = self.client
            .query(
                "SELECT name, position, type, default_expression FROM system.columns WHERE database = ? AND table = ? ORDER BY position"
            )
            .bind(&table.database)
            .bind(&table.table)
            .fetch_all::<ClickhouseTableColumnRow>()
            .await
            .map_err(|e| {
                format!("querying table columns: {}", &e);
                e
            })?;

        Ok(rows)
    }

    pub async fn write_batch(&self, query: String) -> Result<()> {
        self.client.query(&query).execute().await.map_err(|e| {
            format!("inserting batch: {}", &e);
            e
        })?;

        Ok(())
    }
}
