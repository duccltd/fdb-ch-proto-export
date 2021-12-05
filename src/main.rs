use fdb_ch_proto_export::{result::Result, fdb::FdbClient, config, protobuf::load_protobufs};

#[tokio::main]
async fn main() -> Result<()> {
    let mut config = config::load_config().expect("unable to load config");

    let proto_context = match &config.proto_file {
        Some(path) => Some(load_protobufs(&path).await?),
        None => None,
    };

    #[allow(unused)]
    let guard = unsafe { FdbClient::start_network() }.expect("unable to start network");

    let client = FdbClient::new(&config.cluster_file).expect("unable to start client");

    // let clickhouse_table = ClickhouseTable::from_string(table)?;

    Ok(())
}
