use crate::error::Error;
use crate::result::Result;
use serde::{Deserialize, Serialize};
use tracing::*;

const CONFIGURATION_NAME: &str = "fdb-proto-cli";
const VERSION: &str = "0.1.0";

pub fn load_config() -> Result<FdbCliConfig> {
    let config = match confy::load::<FdbCliConfig>(CONFIGURATION_NAME) {
        Ok(res) => {
            info!(
                "Found fdb-ch-proto-export configuration file (version: {:?})",
                res.version
            );
            res
        }
        Err(e) => return Err(Error::UnableToReadConfig(e)),
    };
    Ok(config)
}

pub fn load_mapping() -> Result<Vec<Mapping>> {
    Ok(vec![])
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
            proto_file: None,
            mapping_file: None
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

    pub fn write(&self) -> Result<()> {
        confy::store(CONFIGURATION_NAME, self).map_err(Error::UnableToWriteConfig)
    }
}