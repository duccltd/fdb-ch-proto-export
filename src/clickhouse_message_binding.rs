use protofish::{prelude::MessageValue, context::{MessageField, MessageInfo, ValueType}};

use crate::{clickhouse::ClickhouseTableColumn, clickhouse_table::Table, error::Error};
use lazy_static::lazy_static;
use regex::Regex;
use crate::{result::Result};

lazy_static! {
    static ref ENUM_REGEX: Regex = Regex::new(r"Enum(8|16)\(").unwrap();
    static ref INT_REGEX: Regex = Regex::new(r"(U)?Int(8|16|32|64)").unwrap();
}

pub struct MessageBinding<'a> {
    pub r#type: &'a MessageInfo,
    pub table: Table,
    pub message_mappings: Vec<PreparedMessageField<'a>>
}

fn prepare<'a>(field: &'a MessageField, column: &ClickhouseTableColumn) -> Result<PreparedMessageField<'a>> {
    let nullable = column.name.starts_with("Nullable(");

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
                // TODO: Sub messages - E.g google timestamp
            }

            Ok(())
        }
    )?;
    
    Ok(MessageBinding {
        r#type: message,
        table,
        message_mappings: column_fields
    })
}

impl<'a> MessageBinding<'a> {
    pub fn prepare(&self, message: &MessageValue) -> Result<Vec<String>> {
        let mut results: Vec<String> = Vec::with_capacity(self.message_mappings.len());
        for (idx, field) in self.message_mappings.iter().enumerate() {
            results[idx] = field.prepare_field_value(message)?.unwrap();
        }
        Ok(results)
    }

}

pub struct PreparedMessageField<'a> {
    desc: &'a MessageField,
    kind: ValueType,

    nullable: bool,
    int_size: i32,
    is_timestamp: bool,
    is_datetime_64: bool,
    timestamp_fields: Vec<String>,

    default_expression: String,
}

impl<'a> PreparedMessageField<'a> {
    pub fn prepare_field_value<T>(
        &self, 
        _message: &MessageValue
    ) -> Result<Option<T>> {
        // let field_value = match message.fields.iter().find(|f| f.number == field.desc.number) {
        //     Some(value) => value,
        //     None => {
        //         if field.nullable {
        //             return Ok(None);
        //         } else {
        //             // TODO: Return default type
        //             return Ok(None)
        //         }
        //     }
        // };

        // if field.is_timestamp {
        //     return Ok(None)
        // } else if field.int_size != 0 {
        //     // Check kinds and deserialize
        //     return Ok(None)
        // } else {
        //     return Ok(Some(field_value.value))
        // }
        Ok(None)

    }
}
