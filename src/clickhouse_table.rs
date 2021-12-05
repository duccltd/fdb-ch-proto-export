use crate::{result::Result, error::Error};

pub struct ClickhouseTable {
    pub database: String,
    pub table: String
}

impl ClickhouseTable {
    pub fn from_string(entry: &str) -> Result<ClickhouseTable> {
        let parts: Vec<&str> = entry.split('.').collect();
        if parts.len() != 2 {
            return Err(Error::InvalidMappingConfig("Invalid table definition. Must specify <database>.<table_name>.".to_string()))
        }
        Ok(ClickhouseTable {
            database: parts[0].to_string(),
            table: parts[1].to_string()
        })
    }
}