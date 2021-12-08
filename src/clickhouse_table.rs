use std::collections::HashMap;

use protofish::prelude::{MessageValue, Context, Value};

use crate::{result::Result, error::Error, clickhouse::{Client, ClickhouseTableColumn}, protobuf::value_to_string};

#[derive(Clone)]
pub struct ClickhouseTableParts {
    pub database: String,
    pub table: String
}

impl std::fmt::Display for ClickhouseTableParts {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}.{}", self.database, self.table)
    }
}

impl ClickhouseTableParts {
    pub fn from_string(entry: &str) -> Result<ClickhouseTableParts> {
        let parts: Vec<&str> = entry.split('.').collect();
        if parts.len() != 2 {
            return Err(Error::InvalidMappingConfig("Invalid table definition. Must specify <database>.<table_name>.".to_string()))
        }
        Ok(ClickhouseTableParts {
            database: parts[0].to_string(),
            table: parts[1].to_string()
        })
    }
}

pub async fn construct_table(ch: &Client, table_name: &String) -> Result<Table> {
    let table = ClickhouseTableParts::from_string(&table_name)?;

    let columns = ch.table_columns(table.clone()).await?;

    Ok(Table::new(table, columns))
}


pub struct Table {
    pub parts: ClickhouseTableParts,
    pub columns: Vec<ClickhouseTableColumn>,
}

impl Table {
    pub fn new(parts: ClickhouseTableParts, columns: Vec<ClickhouseTableColumn>) -> Table {
        Table { 
            parts: parts.clone(),
            columns: columns.clone(), 
        }
    }

    pub fn column_values(&self) -> Vec<String> {
        self.columns.iter().map(|e| e.name.clone()).collect()
    }

    pub fn construct_query(
        &self, 
        ctx: &Context,
        fields: Vec<Value>
    ) -> Result<String> {
        let names = self.column_values();
        // let placeholders: Vec<String> = names.iter().map(|_| "{}".to_string()).collect();
        // let query = format!("({})", placeholders.join(","));

        let mut values: Vec<String> = Vec::with_capacity(fields.len());
        for (i, entry) in fields.iter().enumerate() {

            let part = value_to_string(ctx, entry)?;

            values.insert(i, part);
        }

        Ok(format!("INSERT INTO {} ({}) VALUES ({})", self.parts.to_string(), names.join(","), values.join(",")))
    }
}