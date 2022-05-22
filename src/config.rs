use std::{fs::File, io::Read};

use crate::error::Error;
use crate::result::Result;
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use tracing::*;

lazy_static! {
    pub static ref CONFIGURATION_PATH: String = std::env::var("CONFIGURATION_PATH")
        .unwrap_or_else(|_| "fdb-ch-proto-export.toml".to_owned());
}

const VERSION: &str = "0.1.0";

pub fn load_config() -> Result<FdbCliConfig> {
    let config = match confy::load::<FdbCliConfig>(&CONFIGURATION_PATH.to_string()) {
        Ok(res) => {
            info!(
                "Found fdb-ch-proto-export configuration file (version: {:?})",
                res.version
            );

            // Defaults that are all overidable
            let cluster_file = match std::env::var("FDB_CLUSTER_FILE") {
                Ok(cluster_file) => {
                    info!(
                        "Found environment variable override for FDB_CLUSTER_FILE: {}",
                        &cluster_file
                    );
                    cluster_file
                }
                Err(_e) => res.cluster_file,
            };

            let database_url = match std::env::var("DATABASE_URL") {
                Ok(database_url) => {
                    info!(
                        "Found environment variable override for DATABASE_URL: {}",
                        &database_url
                    );
                    database_url
                }
                Err(_e) => res.database_url,
            };

            let proto_file = match std::env::var("PROTO_FILE") {
                Ok(proto_file) => {
                    info!(
                        "Found environment variable override for PROTO_FILE: {}",
                        &proto_file
                    );
                    Some(proto_file)
                }
                Err(_e) => res.proto_file,
            };

            let mapping_file = match std::env::var("MAPPING_FILE") {
                Ok(mapping_file) => {
                    info!(
                        "Found environment variable override for MAPPING_FILE: {}",
                        &mapping_file
                    );
                    Some(mapping_file)
                }
                Err(_e) => res.mapping_file,
            };

            #[cfg(feature = "secure")]
            let postgres_ca_file = match std::env::var("POSTGRES_CA_FILE") {
                Ok(postgres_ca_file) => {
                    info!(
                        "Found environment variable override for POSTGRES_CA_FILE: {}",
                        &postgres_ca_file
                    );
                    Some(postgres_ca_file)
                }
                Err(_e) => res.postgres_ca_file,
            };

            #[cfg(feature = "secure")]
            let postgres_certificate_chain_file = match std::env::var("POSTGRES_CERTIFICATE_CHAIN_FILE") {
                Ok(postgres_certificate_chain_file) => {
                    info!(
                        "Found environment variable override for POSTGRES_CERTIFICATE_CHAIN_FILE: {}",
                        &postgres_certificate_chain_file
                    );
                    Some(postgres_certificate_chain_file)
                }
                Err(_e) => res.postgres_certificate_chain_file,
            };

            #[cfg(feature = "secure")]
            let postgres_private_key_file = match std::env::var("POSTGRES_PRIVATE_KEY_FILE") {
                Ok(postgres_private_key_file) => {
                    info!(
                        "Found environment variable override for POSTGRES_PRIVATE_KEY_FILE: {}",
                        &postgres_private_key_file
                    );
                    Some(postgres_private_key_file)
                }
                Err(_e) => res.postgres_private_key_file,
            };

            FdbCliConfig {
                cluster_file,
                database_url,
                proto_file,
                mapping_file,
                #[cfg(feature = "secure")]
                postgres_ca_file,
                #[cfg(feature = "secure")]
                postgres_certificate_chain_file,
                #[cfg(feature = "secure")]
                postgres_private_key_file,
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
    pub table: String,
    pub custom_field_mapping: Option<Vec<CustomFieldMapping>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CustomFieldMapping {
    pub from: String,
    pub to: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct FdbCliConfig {
    // fdb-cli version
    version: String,

    // path to cluster file
    pub cluster_file: String,

    // clickhouse url
    pub database_url: String,

    // path to the protobuf file
    pub proto_file: Option<String>,

    // path to mapping proto config
    pub mapping_file: Option<String>,

    // path to secure mode certificate authority file
    #[cfg(feature = "secure")]
    pub postgres_ca_file: Option<String>,

    // path to secure mode certificate chain file
    #[cfg(feature = "secure")]
    pub postgres_certificate_chain_file: Option<String>,

    // path to secure mode private key file
    #[cfg(feature = "secure")]
    pub postgres_private_key_file: Option<String>,
}

impl std::default::Default for FdbCliConfig {
    fn default() -> Self {
        let path = FdbCliConfig::default_cluster_file();

        Self {
            version: VERSION.to_string(),
            cluster_file: String::from(path),
            database_url: "http://localhost:8083".to_string(),
            proto_file: None,
            mapping_file: None,
            #[cfg(feature = "secure")]
            postgres_ca_file: None,
            #[cfg(feature = "secure")]
            postgres_certificate_chain_file: None,
            #[cfg(feature = "secure")]
            postgres_private_key_file: None,
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
                debug!("Using mapping file path: {}", file);
                file
            }
            None => return Err(Error::MissingConfig("Mapping config not provided".into())),
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
