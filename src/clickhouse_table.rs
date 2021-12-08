use std::collections::HashMap;

use protofish::context::{MessageInfo, MessageField, ValueType};
use lazy_static::lazy_static;
use regex::Regex;
use crate::{result::Result, error::Error, clickhouse::{Client, ClickhouseTableColumn}};

lazy_static! {
    static ref ENUM_REGEX: Regex = Regex::new(r"Enum(8|16)\(").unwrap();
    static ref INT_REGEX: Regex = Regex::new(r"(U)?Int(8|16|32|64)").unwrap();
}

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

pub struct MessageBinding<'a> {
    pub r#type: &'a MessageInfo,
    pub table: Table,
}

fn prepare<'a>(field: &'a MessageField, column: &ClickhouseTableColumn) -> Result<PreparedMessageField<'a>> {
    let nullable = column.name.starts_with("Nullable(");
    let is_array = column.name.starts_with("Array(");

    let mut int_size = 0;

    let matches: Vec<regex::Captures> = INT_REGEX.captures_iter(&column.name).collect();
    if matches.len() == 1 {
        match matches[0].get(2) {
            Some(entry) => {
                int_size = match entry.as_str().parse::<i32>() {
                    Ok(i) => i,
                    Err(_e) => return Err(Error::ParseError("Invalid integer prefix".into()))
                };
                entry
            },
            None => return Err(Error::ParseError("Unable to parse integer type".into()))
        };

        if let Some(delimiter) = matches[0].get(1) {
            if delimiter.as_str() == "U" {
                int_size = int_size * -1;
            }
        }
    }

    let matches: Vec<regex::Captures> = ENUM_REGEX.captures_iter(&column.name).collect();
    if matches.len() == 1 {
        match matches[0].get(1) {
            Some(entry) => {
                int_size = match entry.as_str().parse::<i32>() {
                    Ok(i) => i,
                    Err(_e) => return Err(Error::ParseError("Invalid integer prefix".into()))
                };
            },
            None => return Err(Error::ParseError("Unable to parse integer type".into()))
        };

        int_size *= -1;
    }

    Ok(PreparedMessageField {
        desc: field,
        kind: field.field_type.clone(),

        nullable,
        int_size,
        is_array,
        is_map_key: todo!(),
        is_map_value: todo!(),
        is_timestamp: todo!(),
        is_datetime_64: todo!(),
        timestamp_fields: todo!(),
        default_expression: column.default_expression,
    })
}

pub fn bind_proto_message(message: &MessageInfo, table: Table) -> Result<MessageBinding> {
    let mut column_fields: Vec<PreparedMessageField> = Vec::with_capacity(table.columns.len());
    message.iter_fields().try_for_each(
        |field| {
            let column_name = &field.name;

            let table_column = match table.columns.iter().find(|c| &c.name == column_name) {
                Some(col) => col,
                // TODO: Error type
                None => return Err(Error::ParseError("Column does not exist".into()))
            };

            column_fields[table_column.position as usize] = prepare(field, table_column)?;

            if ValueType::Message(message.self_ref) == field.field_type  {
                // TODO: Sub messages
            }

            Ok(())
        }
    )?;
    
    Ok(MessageBinding {
        r#type: message,
        table
    })
}

impl<'a> MessageBinding<'a> {
    pub fn prepare(&self) {

    }
}

pub struct PreparedMessageField<'a> {
    desc: &'a MessageField,
    kind: ValueType,

    nullable: bool,
    int_size: i32,
    is_array: bool,
    is_map_key: bool,
    is_map_value: bool,
    is_timestamp: bool,
    is_datetime_64: bool,
    timestamp_fields: Vec<String>,

    default_expression: String,
}

impl<'a> PreparedMessageField<'a> {

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