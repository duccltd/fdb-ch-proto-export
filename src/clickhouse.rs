use std::collections::HashMap;

use crate::clickhouse_table::{ClickhouseTableParts, Table};
use crate::result::Result;
use serde::{Deserialize, Serialize};

#[derive(Debug, clickhouse::Row, Serialize, Deserialize, Clone)]
pub struct ClickhouseTableColumn {
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

    pub async fn table_columns(&self, table: ClickhouseTableParts) -> Result<Vec<ClickhouseTableColumn>> {
        let rows = self.client
            .query(
                "SELECT name, position, type, default_expression FROM system.columns WHERE database = ? AND table = ? ORDER BY position"
            )
            .bind(&table.database)
            .bind(&table.table)
            .fetch_all::<ClickhouseTableColumn>()
            .await
            .map_err(|e| {
                format!("querying table columns: {}", &e);
                e
            })?;

        Ok(rows)
    }

    pub async fn write_batch(&self, table: &Table, batch: Vec<HashMap<String, serde_json::Value>>) -> Result<()> {
        let _query = table.construct_query(batch.clone());

        //

        Ok(())
    }
}