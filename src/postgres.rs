use crate::postgres_table::ClickhouseTableParts;
use crate::result::Result;
use serde::{Deserialize, Serialize};
use tracing::*;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TableColumnRow {
    pub name: String,
    pub position: i64,
    pub r#type: String,
    pub nullable: bool,
    pub default_expression: String,
}

impl From<tokio_postgres::Row> for TableColumnRow {
    fn from(row: tokio_postgres::Row) -> TableColumnRow {
        Self {
            name: row.get("name"),
            position: row.get("position"),
            r#type: row.get("type"),
            default_expression: row.get("default_expression"),
            nullable: row.get("nullable")
        }
    }
}

pub struct Client {
    pub client: tokio_postgres::Client,
}

impl Client {
    pub fn new(client: tokio_postgres::Client) -> Self {
        Self { client }
    }

    pub async fn table_columns(
        &mut self,
        table: ClickhouseTableParts,
    ) -> Result<Vec<TableColumnRow>> {
        let rows = self.client
            .query(
                "SELECT 
                    column_name as name, 
                    column_id as position, 
                    column_type as type,
                    nullable,
                    '' as default_expression
                FROM crdb_internal.table_columns 
                WHERE descriptor_name = $1
                GROUP BY descriptor_name, column_id, column_name, column_type, nullable",
                &[&table.table]
            )
            .await?;

        Ok(rows.into_iter().map(|row| row.into()).collect())
    }

    pub async fn write_batch(&mut self, query: String) -> Result<()> {
        debug!("writing batch: {}", &query);

        self.client.execute(&query, &[]).await.map_err(|e| {
            format!("inserting batch: {}", &e);
            e
        })?;

        Ok(())
    }
}
