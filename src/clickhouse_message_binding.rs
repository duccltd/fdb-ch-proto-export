use std::collections::{BTreeMap, HashMap};

use protofish::{
    context::{MessageField, MessageInfo, ValueType},
    prelude::{Context, MessageValue},
};

use crate::{
    clickhouse_table::{Table, TableColumn},
    error::Error,
    protobuf::value_to_string,
};

use crate::result::Result;
use tracing::*;

pub struct MessageBinding<'a> {
    pub r#type: &'a MessageInfo,
    pub table: Table,
    pub message_mappings: HashMap<usize, PreparedMessageField<'a>>,
}

pub fn bind_proto_message(message: &MessageInfo, table: Table) -> Result<MessageBinding> {
    info!(
        "binding {} to {}. num columns: {}",
        &message.full_name,
        &table.parts.to_string(),
        table.columns.len()
    );

    let mut column_fields: HashMap<usize, PreparedMessageField> = HashMap::new();

    for column in &table.columns {
        match message.iter_fields().find(|f| &f.name == &column.name) {
            Some(field) => {
                column_fields.insert(
                    (column.position - 1) as usize,
                    PreparedMessageField {
                        desc: field,
                        kind: field.field_type.clone(),
                        column: column.clone(),
                    },
                );
            }
            None => continue,
        };
    }

    Ok(MessageBinding {
        r#type: message,
        table,
        message_mappings: column_fields,
    })
}

impl<'a> MessageBinding<'a> {
    pub fn prepare(&self, ctx: &Context, message: &[u8]) -> Result<BTreeMap<usize, String>> {
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
    column: TableColumn,
}

impl<'a> PreparedMessageField<'a> {
    pub fn prepare_field_value(&self, ctx: &Context, message: &MessageValue) -> Result<String> {
        match message.fields.iter().find(|f| f.number == self.desc.number) {
            Some(field_value) => {
                let field_value = field_value.value.clone();
                let value = value_to_string(ctx, &field_value)?;

                Ok(match self.kind {
                    ValueType::Message(_) => format!("'{}'", value),
                    _ => value,
                })
            }
            None => match self.column.default() {
                Some(value) => return Ok(value),
                None => Ok(match self.kind {
                    ValueType::Bool => "false".to_string(),
                    ValueType::String => "''".to_string(),
                    ValueType::Message(_) => "{}".to_string(),
                    ValueType::Enum(_) => "''".to_string(),
                    _ => {
                        return Err(Error::NoProtoDefault(format!(
                            "For '{}' in message",
                            self.desc.name
                        )))
                    }
                }),
            },
        }
    }
}
