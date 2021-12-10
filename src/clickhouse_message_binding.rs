use std::collections::{HashMap, BTreeMap};

use protofish::{prelude::{MessageValue, Context}, context::{MessageField, MessageInfo, ValueType}};

use crate::{clickhouse::ClickhouseTableColumn, clickhouse_table::Table, error::Error, protobuf::value_to_string};
use lazy_static::lazy_static;
use regex::Regex;
use crate::{result::Result};
use tracing::*;

lazy_static! {
    static ref ENUM_REGEX: Regex = Regex::new(r"Enum(8|16)\(").unwrap();
    static ref INT_REGEX: Regex = Regex::new(r"(U)?Int(8|16|32|64)").unwrap();
}

pub struct MessageBinding<'a> {
    pub r#type: &'a MessageInfo,
    pub table: Table,
    pub message_mappings: HashMap<usize, PreparedMessageField<'a>>
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
        default_expression: column.default_expression.clone(),
    })
}

pub fn bind_proto_message(message: &MessageInfo, table: Table) -> Result<MessageBinding> {
    info!("binding {} to {}. num columns: {}", &message.full_name, &table.parts.to_string(), table.columns.len());

    let mut column_fields: HashMap<usize, PreparedMessageField> = HashMap::new();

    for field in message.iter_fields() {
        let column_name = &field.name;

        let table_column = match table.columns.iter().find(|c| &c.name == column_name) {
            Some(col) => col,
            None => continue
        };

        column_fields.insert((table_column.position - 1) as usize, prepare(field, table_column)?);
    }
    
    Ok(MessageBinding {
        r#type: message,
        table,
        message_mappings: column_fields
    })
}

impl<'a> MessageBinding<'a> {
    pub fn prepare(
        &self,
        ctx: &Context,
        message: &[u8]
    ) -> Result<BTreeMap<usize, String>> {
        let data = self.r#type.decode(message, ctx);

        let mut results: BTreeMap<usize, String> = BTreeMap::new();
        for (idx, field) in &self.message_mappings {
            let value = field.prepare_field_value(ctx, &data)?;

            results.insert(idx.clone(), value);
        }

        Ok(results)
    }

}

pub struct PreparedMessageField<'a> {
    desc: &'a MessageField,
    kind: ValueType,

    nullable: bool,
    _int_size: i32,
    default_expression: String,
}

impl<'a> PreparedMessageField<'a> {
    pub fn prepare_field_value(
        &self, 
        ctx: &Context,
        message: &MessageValue
    ) -> Result<String> {
        match message
                .fields
                .iter()
                .find(|f| f.number == self.desc.number) {
            Some(field_value) => {
                let field_value = field_value.value.clone();

                value_to_string(ctx, &field_value)
            }
            None => {
                if self.nullable {
                    return Ok("NULL".to_string());
                }
                if self.default_expression != "" {
                    return Ok(self.default_expression.clone())
                }
                Ok(match self.kind {
                    ValueType::Bool => "false".to_string(),
                    ValueType::String => "''".to_string(),
                    ValueType::Message(_) => "{}".to_string(),
                    // TODO: New error
                    _ => return Err(Error::ParseError(format!("Could not find field or produce default for '{}' in message", self.desc.name)))
                })
            }
        }
    }
}
