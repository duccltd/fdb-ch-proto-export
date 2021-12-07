use std::collections::HashMap;

use protofish::context::{MessageInfo, MessageField};

use crate::{result::Result, error::Error, config::Mapping, clickhouse::{Client, ClickhouseTableColumn}};

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

pub struct MessageBinding {
    // pub r#type: MessageInfo,
    pub table: Table,
}

pub fn bind_proto_message(message: &MessageInfo, table: Table) -> Result<MessageBinding> {
    message.iter_fields().for_each(
        |field| {

        }
    );
    
    Ok(MessageBinding {
        // r#type: message,
        table
    })
}

impl MessageBinding {
    pub fn prepare(&self) {

    }
}

pub struct MessagePreperation {
    desc: String,
    kind: String,

    nullable: bool,
    int_size: i32,
    is_array: bool,
    is_map_key: bool,
    is_map_value: bool,
    is_timestamp: bool,
    is_datetime_64: bool,
    timestamp_fields: Vec<String>
}

impl MessagePreperation {

}

pub struct Table {
    pub parts: ClickhouseTableParts,
    pub columns: Vec<ClickhouseTableColumn>,
    pub binding: Option<MessagePreperation>,
}

impl Table {
    pub fn new(parts: ClickhouseTableParts, columns: Vec<ClickhouseTableColumn>) -> Table {
        Table { 
            parts: parts.clone(),
            columns: columns.clone(), 
            binding: None
        }
    }

    pub fn column_values(&self) -> Vec<String> {
        self.columns.iter().map(|e| e.name.clone()).collect()
    }

    pub fn bind_message(&self, message: MessageInfo) -> Result<()> {
        let message_name = message.full_name;

        Ok(())

    }

    pub fn construct_query(&self, entries: Vec<HashMap<String, serde_json::Value>>) -> String {
        let names: Vec<String> = self.columns.iter().map(|e| e.name.clone()).collect();
        let placeholders: Vec<String> = names.iter().map(|_| "?".to_string()).collect();
        let query = format!("({})", placeholders.join(","));

        let mut values: Vec<String> = Vec::with_capacity(entries.len());
        for i in 0..entries.len() {
            values[i] = query.clone();
        }

        format!("INSERT INTO {} ({}) VALUES ({})", self.parts.to_string(), names.join(","), values.join(","))
    }
}