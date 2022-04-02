use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "fdb-cli", about = "foundation db cli tool")]
pub enum Opts {
    // Setup a foundation db instance
    Setup(Setup),

    Export,
}

#[derive(Debug, StructOpt)]
pub enum Setup {
    Set(Set),

    View,
}

#[derive(Debug, StructOpt)]
pub struct Set {
    #[structopt(long, help = "Path to cluster file")]
    pub cluster_file: Option<String>,

    #[structopt(long, help = "Path to the protobuf file")]
    pub proto_file: Option<String>,

    #[structopt(long, help = "Clickhouse url")]
    pub database_url: Option<String>,

    #[structopt(long, help = "Path to the mapping")]
    pub mapping_file: Option<String>,
}

pub fn parse() -> Opts {
    Opts::from_args()
}
