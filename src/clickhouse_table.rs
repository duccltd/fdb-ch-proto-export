use std::collections::BTreeMap;

use regex::Regex;
use serde::{Deserialize, Serialize};
use tracing::*;

use crate::{clickhouse::ClickhouseTableColumnRow, error::Error, result::Result};
use lazy_static::lazy_static;

lazy_static! {
    static ref ENUM_REGEX: Regex = Regex::new(r"Enum(8|16)\(").unwrap();
    static ref INT_REGEX: Regex = Regex::new(r"(U)?Int(8|16|32|64)").unwrap();
}

#[derive(Clone)]
pub struct ClickhouseTableParts {
    pub database: String,
    pub table: String,
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
            return Err(Error::InvalidMappingConfig(
                "Invalid table definition. Must specify <database>.<table_name>.".to_string(),
            ));
        }
        Ok(ClickhouseTableParts {
            database: parts[0].to_string(),
            table: parts[1].to_string(),
        })
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct TableColumn {
    pub name: String,
    pub position: u64,
    pub r#type: String,
    pub default_expression: String,

    pub nullable: bool,
    pub _int_size: i32,
}

impl TableColumn {
    pub fn default(&self) -> Option<String> {
        if self.nullable {
            return Some("NULL".to_string());
        }
        if self.default_expression != "" {
            return Some(self.default_expression.clone());
        }
        match self.r#type.as_ref() {
            "String" => {
                return Some("''".to_string());
            }
            _ => None,
        }
    }
}

impl TryFrom<ClickhouseTableColumnRow> for TableColumn {
    type Error = crate::error::Error;

    fn try_from(value: ClickhouseTableColumnRow) -> Result<Self> {
        let nullable = value.name.starts_with("Nullable(");

        let mut int_size = 0;

        let matches: Vec<regex::Captures> = INT_REGEX.captures_iter(&value.name).collect();
        if matches.len() == 1 {
            match matches[0].get(2) {
                Some(entry) => {
                    int_size = match entry.as_str().parse::<i32>() {
                        Ok(i) => i,
                        Err(_e) => return Err(Error::ParseError("Invalid integer prefix".into())),
                    };
                    entry
                }
                None => return Err(Error::ParseError("Unable to parse integer type".into())),
            };

            if let Some(delimiter) = matches[0].get(1) {
                if delimiter.as_str() == "U" {
                    int_size = int_size * -1;
                }
            }
        }

        let matches: Vec<regex::Captures> = ENUM_REGEX.captures_iter(&value.name).collect();
        if matches.len() == 1 {
            match matches[0].get(1) {
                Some(entry) => {
                    int_size = match entry.as_str().parse::<i32>() {
                        Ok(i) => i,
                        Err(_e) => return Err(Error::ParseError("Invalid integer prefix".into())),
                    };
                }
                None => return Err(Error::ParseError("Unable to parse integer type".into())),
            };

            int_size *= -1;
        }

        Ok(TableColumn {
            name: value.name,
            default_expression: value.default_expression,
            r#type: value.r#type,
            position: value.position,
            nullable,
            _int_size: int_size,
        })
    }
}

pub struct Table {
    pub parts: ClickhouseTableParts,
    pub columns: Vec<TableColumn>,
}

impl Table {
    pub fn new(parts: ClickhouseTableParts, columns: Vec<TableColumn>) -> Table {
        Table {
            parts: parts.clone(),
            columns: columns.clone(),
        }
    }

    pub fn column_values(&self) -> Vec<String> {
        self.columns.iter().map(|e| e.name.clone()).collect()
    }

    pub fn construct_query(&self, fields: BTreeMap<usize, String>) -> Result<String> {
        let names = self.column_values();

        let values = fields.values().cloned().collect::<Vec<String>>();

        Ok(format!(
            "INSERT INTO {} ({}) VALUES ({})",
            self.parts.to_string(),
            names.join(","),
            values.join(",")
        ))
    }

    pub fn construct_batch(&self, entries: Vec<BTreeMap<usize, String>>) -> Result<String> {
        let names = self.column_values();

        let mut parts: Vec<String> = vec![];

        for (_idx, entry) in entries.iter().enumerate() {
            let mut current_part: Vec<String> = vec![];

            for i in 0..self.columns.len() {
                let column = &self.columns[i];

                let value = match entry.get(&i) {
                    Some(value) => value.clone(),
                    None => match column.default() {
                        Some(default) => default,
                        None => {
                            info!(
                                "Discarded message, missing column value: table={} col={}",
                                &self.parts.to_string(),
                                &column.name
                            );
                            break;
                        }
                    },
                };

                current_part.push(value);
            }

            parts.push(format!("({})", current_part.join(",")));
        }

        Ok(format!(
            "INSERT INTO {} ({}) VALUES {}",
            self.parts.to_string(),
            names.join(","),
            parts.join(",")
        ))
    }
}
