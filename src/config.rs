use std::{fs::File, io::Read};

use crate::error::Error;
use crate::result::Result;
use serde::{Deserialize, Serialize};
use tracing::*;
use lazy_static::lazy_static;

lazy_static! {
    pub static ref CONFIGURATION_PATH: String =
        std::env::var("CONFIGURATION_PATH").unwrap_or_else(|_| "fdb-ch-proto-export.toml".to_owned());
}

const VERSION: &str = "0.1.0";

fn get_environment_var<T>(var: &str, default: T) -> Result<T> {
    let env_var = std::env::var(var);
    if env_var.is_ok() {
        info!("Found environment variable override for {}: {}", var, env_var.clone().unwrap());
        env_var.unwrap();
    }
    Ok(default)
}

pub fn load_config() -> Result<FdbCliConfig> {
    let config = match confy::load::<FdbCliConfig>(&CONFIGURATION_PATH.to_string()) {
        Ok(res) => {
            info!(
                "Found fdb-ch-proto-export configuration file (version: {:?})",
                res.version
            );

            // Defaults that are all overidable
            let cluster_file = get_environment_var("FDB_CLUSTER_FILE", res.cluster_file)?;
            let clickhouse_url = get_environment_var("CLICKHOUSE_URL", res.clickhouse_url)?;
            let proto_file = get_environment_var("PROTO_FILE", res.proto_file)?;
            let mapping_file = get_environment_var("MAPPING_FILE", res.mapping_file)?;

            FdbCliConfig {
                cluster_file,
                clickhouse_url,
                proto_file,
                mapping_file,
                ..res
            }
        }
        Err(e) => return Err(Error::UnableToReadConfig(e)),
    };
    Ok(config)
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Mapping {
    pub from: String,
    pub to: String,
    pub proto: String,
    pub table: String
}

#[derive(Serialize, Deserialize, Debug)]
pub struct FdbCliConfig {
    // fdb-cli version
    version: String,

    // path to cluster file
    pub cluster_file: String,

    // clickhouse url
    pub clickhouse_url: String,

    // path to the protobuf file
    pub proto_file: Option<String>,

    // path to mapping proto config
    pub mapping_file: Option<String>
}

impl std::default::Default for FdbCliConfig {
    fn default() -> Self {
        let path = FdbCliConfig::default_cluster_file();

        Self {
            version: VERSION.to_string(),
            cluster_file: String::from(path),
            clickhouse_url: "http://localhost:8083".to_string(),
            proto_file: None,
            mapping_file: None,
        }
    }
}

impl FdbCliConfig {
    pub fn default_cluster_file() -> &'static str {
        let os_type = os_type::current_platform().os_type;
        match os_type {
            // OSX Path
            os_type::OSType::OSX => "/usr/local/etc/foundationdb/fdb.cluster",
            // All other types are unix based systems
            _ => "/etc/foundationdb/fdb.cluster",
        }
    }

    pub fn load_mapping(&self) -> Result<Vec<Mapping>> {
        let mapping_file = match &self.mapping_file {
            Some(file) => {
                info!("Using mapping file path: {}", file);
                file
            },
            None => return Err(Error::MissingConfig("Mapping config not provided".into()))
        };

        let mut file = File::open(&mapping_file)?;
        let mut data = String::new();

        file.read_to_string(&mut data)?;

        Ok(serde_json::from_str::<Vec<Mapping>>(&data)?)
    }

    pub fn write(&self) -> Result<()> {
        confy::store(&CONFIGURATION_PATH.to_string(), self).map_err(Error::UnableToWriteConfig)
    }
}