use protofish::{context::MessageInfo, prelude::Context};

use crate::{result::Result, config::Mapping, error::Error, clickhouse_table::{ClickhouseTableParts, Table, bind_proto_message}, context::{AppContext}};

impl AppContext {
    pub async fn bind_messages(
        &mut self,
        mappings: &Vec<Mapping>,
        proto_context: Context
    ) -> Result<()> {
        for mapping in mappings {
            let message = match proto_context.get_message(&mapping.proto) {
                Some(message) => message,
                None => return Err(Error::ParseError("Could not find message definition".into()))
            };

            if let Some(binding) = self.proto_registry.get(&message.full_name) {
                let curr_binding = &binding.table.parts;

                return Err(
                    Error::InvalidMappingConfig(
                        format!("Message {} is already binded to {}", &message.full_name, curr_binding.to_string())
                    )
                )
            }

            self.bind_message(message, &mapping.table).await?;
        }

        Ok(())
    }

    async fn bind_message(
        &mut self,
        message: &MessageInfo, 
        table_name: &String
    ) -> Result<()> {
        let table = match self.construct_table(table_name).await {
            Ok(table) => table,
            Err(_e) => return Err(Error::ParseError("Unable to fetch table columns.".into()))
        };

        let binding = match bind_proto_message(table) {
            Ok(binding) => binding,
            Err(_e) => return Err(Error::ParseError("Could not create binding.".into()))
        };
        
        self.proto_registry.insert(message.full_name.clone(), binding);

        Ok(())
    }

    async fn construct_table(
        &self,
        table_name: &String
    ) -> Result<Table> {
        let table = ClickhouseTableParts::from_string(&table_name)?;

        let columns = self.ch_client.table_columns(table.clone()).await?;

        Ok(Table::new(table, columns))
    }
}