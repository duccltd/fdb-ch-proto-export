use std::sync::Arc;

use clickhouse::Client;
use fdb_ch_proto_export::context::AppContext;
use fdb_ch_proto_export::cli;
use fdb_ch_proto_export::{result::Result, fdb::FdbClient, config, error::Error, protobuf::load_protobufs, clickhouse::Client as ClickhouseClient};
use foundationdb::RangeOption;
use futures::StreamExt;
use tracing::*;


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

                if let Some(clickhouse_url) = set.clickhouse_url {
                    config.clickhouse_url = clickhouse_url;
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
                    println!("Options are cluster-file, proto-file and mapping-file")
                }
            }
            cli::Setup::View => {
                println!("{:?}", config);
            }
        }
        cli::Opts::Export => {
            let proto_context = match &config.proto_file {
                Some(path) => load_protobufs(&path).await?,
                None => return Err(Error::ParseError("Missing protofile definition".into())),
            };

            #[allow(unused)]
            let guard = unsafe { FdbClient::start_network() }.expect("unable to start network");

            let client = Arc::new(FdbClient::new(&config.cluster_file).expect("unable to start client"));

            let ch_client = ClickhouseClient::new(Client::default().with_url(&config.clickhouse_url));

            let mapping = &config.load_mapping().expect("unable to read mapping config");

            let mut context = AppContext::new(client.clone(), ch_client);

            context
                .bind_messages(mapping, &proto_context).await.expect("unable to create registry");

            for map in mapping {
                let tx = client.begin_tx().await.expect("unable to begin tx");
                
                let mut kvs = tx.get_ranges(
                    RangeOption {
                        reverse: false,
                        limit: None,
                        ..RangeOption::from((map.from.as_bytes(), map.to.as_bytes()))
                    },
                    false,
                );

                while let Some(kv) = kvs.next().await {
                    let kv = match kv {
                        Ok(kv) => kv,
                        Err(e) => return Err(Error::Fdb(e)),
                    };

                    for value in (*kv).into_iter() {
                        let v = value.value();

                        let binding = match context.proto_registry.get(&map.proto) {
                            Some(binding) => binding,
                            None => continue
                        };

                        let fields = match binding.prepare(&proto_context, v) {
                            Ok(res) => res,
                            Err(_e) => continue
                        };

                        let query = match binding.table.construct_query(&proto_context, fields) {
                            Ok(query) => query,
                            Err(_e) => continue
                        };

                        println!("{}", query);
                        // context.ch_client.write_batch(query).await?;
                    }

                }
            }
        }
    }

    Ok(())
}
