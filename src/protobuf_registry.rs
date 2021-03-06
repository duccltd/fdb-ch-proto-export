use protofish::{context::MessageInfo, prelude::Context};
use tracing::info;

use crate::{
    clickhouse_message_binding::bind_proto_message,
    clickhouse_table::{ClickhouseTableParts, Table},
    config::Mapping,
    context::AppContext,
    error::Error,
    result::Result,
};

impl<'a> AppContext<'a> {
    pub async fn bind_messages(
        &mut self,
        mappings: &Vec<Mapping>,
        proto_context: &'a Context,
    ) -> Result<()> {
        for mapping in mappings {
            let message = match proto_context.get_message(&mapping.proto) {
                Some(message) => message,
                None => {
                    return Err(Error::ParseError(
                        "Could not find message definition".into(),
                    ))
                }
            };

            if let Some(binding) = self.proto_registry.get(&message.full_name) {
                let curr_binding = &binding.table.parts;

                return Err(Error::InvalidMappingConfig(format!(
                    "Message {} is already binded to {}",
                    &message.full_name,
                    curr_binding.to_string()
                )));
            }

            let table = self.construct_table(&mapping.table).await?;
            if table.columns.len() == 0 {
                info!(
                    "Skipping mapping for table as found no columns: table={}",
                    &table.parts.table
                );
                continue;
            }

            self.bind_message(message, table).await?;
        }

        Ok(())
    }

    async fn bind_message(&mut self, message: &'a MessageInfo, table: Table) -> Result<()> {
        let binding = bind_proto_message(message, table)?;

        self.proto_registry
            .insert(message.full_name.clone(), binding);

        Ok(())
    }

    async fn construct_table(&self, table_name: &String) -> Result<Table> {
        let table = ClickhouseTableParts::from_string(&table_name)?;

        let mut columns = vec![];
        for column in self.ch_client.table_columns(table.clone()).await? {
            columns.push(column.try_into()?);
        }

        Ok(Table::new(table, columns))
    }
}
