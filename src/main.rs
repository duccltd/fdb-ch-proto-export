use std::collections::BTreeMap;
use std::sync::Arc;

use fdb_ch_proto_export::cli;
use fdb_ch_proto_export::context::AppContext;
use fdb_ch_proto_export::{
    config, error::Error, fdb::FdbClient,
    protobuf::load_protobufs, result::Result,
};
use foundationdb::RangeOption;
use futures::StreamExt;
use tracing::*;

#[cfg(feature = "secure")]
use openssl::ssl::{SslConnector, SslMethod, SslFiletype};

#[cfg(not(feature = "secure"))]
use tokio_postgres::{NoTls};

use fdb_ch_proto_export::postgres::Client as PostgresClient;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let mut config = config::load_config().expect("unable to load config");

    let opts = cli::parse();

    match opts {
        cli::Opts::Setup(params) => match params {
            cli::Setup::Set(set) => {
                let mut changed = false;
                if let Some(cluster_file) = set.cluster_file {
                    config.cluster_file = cluster_file;
                    changed = true;
                }

                if let Some(proto_file) = set.proto_file {
                    config.proto_file = Some(proto_file);
                    changed = true;
                }

                if let Some(database_url) = set.database_url {
                    config.database_url = database_url;
                    changed = true;
                }

                if let Some(mapping_file) = set.mapping_file {
                    config.mapping_file = Some(mapping_file);
                    changed = true;
                }

                if changed {
                    match config.write() {
                        Ok(()) => info!("config file has been changed"),
                        Err(e) => panic!("writing config file: {}", e),
                    }
                } else {
                    info!("Options are cluster-file, proto-file and mapping-file")
                }
            }
            cli::Setup::View => {
                info!("{:?}", config);
            }
        },
        cli::Opts::Export => {
            let proto_context = match &config.proto_file {
                Some(path) => {
                    debug!("Using protofile path: {}", path);
                    load_protobufs(&path).await?
                }
                None => return Err(Error::MissingConfig("Missing protofile definition".into())),
            };

            #[allow(unused)]
            let guard = unsafe { FdbClient::start_network() }.expect("unable to start network");

            debug!("Using fdb cluster file path: {}", &config.cluster_file);

            let client =
                Arc::new(FdbClient::new(&config.cluster_file).expect("unable to start client"));

            debug!("Using cockroach url: {}", &config.database_url);

            #[cfg(feature = "secure")]
            let (pg_client, connection) = {
                let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
                builder.set_ca_file(config.postgres_ca_file.unwrap()).unwrap();
                builder.set_certificate_chain_file(config.postgres_certificate_chain_file.unwrap()).unwrap();
                builder.set_private_key_file(config.postgres_private_key_file.unwrap(), SslFiletype::PEM).unwrap();
                let tls = postgres_openssl::MakeTlsConnector::new(builder.build());

                tokio_postgres::connect(
                    &config.database_url,
                    tls,
                ).await.expect("unable to connect (secure mode)")
            };

            #[cfg(not(feature = "secure"))]
            let (pg_client, connection) = tokio_postgres::connect(
                &config.database_url,
                NoTls,
            ).await.expect("unable to connect (isnecure mode)");

            // The connection object performs the actual communication with the database,
            // so spawn it off to run on its own.
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    error!("connection error: {}", e);
                }
            });    

            let pg_client = PostgresClient::new(pg_client);

            let mapping = &config
                .load_mapping()
                .expect("unable to read mapping config");

            let mut context = AppContext::new(client.clone(), pg_client);

            context
                .bind_messages(mapping, &proto_context)
                .await
                .expect("unable to create registry");

            for map in mapping {
                let binding = match context.proto_registry.get(&map.proto) {
                    Some(binding) => binding,
                    None => continue,
                };

                let mut messages_written = 0;

                let mut from = map.from.as_bytes().to_vec();
                let to = map.to.as_bytes();
                let mut retry = false;

                'retry: loop {
                    let tx = client.begin_tx().await.expect("unable to begin tx");

                    let range_from = from.clone();

                    let mut kvs = tx.get_ranges(
                        RangeOption {
                            reverse: false,
                            limit: None,
                            ..RangeOption::from((range_from.as_ref(), to))
                        },
                        false,
                    );

                    while let Some(kv) = kvs.next().await {
                        let kv = match kv {
                            Ok(kv) => kv,
                            Err(e) => {
                                // 1007: Transaction is too old to perform reads or be committed
                                // We restart the transaction from the last read key.
                                if e.code() == 1007 {
                                    retry = true;
                                    continue 'retry;
                                }

                                return Err(Error::Fdb(e));
                            }
                        };

                        // TODO: Extract batch writing out
                        let mut batch: Vec<BTreeMap<usize, String>> = vec![];

                        let mut last_read_key = None;

                        for value in (*kv).into_iter() {
                            if retry {
                                // If there has been a retry (due to code 1007) skip the first key as the last read captured it
                                retry = false;
                                continue;
                            }

                            let k = value.key().to_vec();
                            let v = value.value();

                            let fields = match binding.prepare(&proto_context, v) {
                                Ok(res) => res,
                                Err(e) => {
                                    error!("Failed transforming message: {:?}", e);
                                    continue;
                                }
                            };

                            last_read_key = Some(k);
                            batch.push(fields);
                        }

                        if batch.len() == 0 {
                            continue;
                        }

                        let query = match binding.table.construct_batch(batch.clone()) {
                            Ok(query) => query,
                            Err(_e) => continue,
                        };

                        context.pg_client.write_batch(query).await?;

                        if let Some(last_read_key) = last_read_key {
                            from = last_read_key;
                        }

                        messages_written += batch.len()
                    }

                    // We have read all the keys in this range
                    break;
                }

                info!(
                    "{} messages written to {}",
                    messages_written,
                    binding.table.parts.to_string()
                )
            }
        }
    }

    Ok(())
}
