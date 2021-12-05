use std::{fmt::Formatter, sync::Arc};

use foundationdb::FdbError;

#[derive(Debug)]
pub enum Error {
    UnableToReadProtobuf(std::io::Error),
    ProtofishParseError(protofish::context::ParseError),
    Fdb(FdbError),
    Elapsed(tokio::time::error::Elapsed),
    UnableToReadConfig(std::io::Error),
    UnableToWriteConfig(std::io::Error),
    InvalidMappingConfig(String),
    Clickhouse(Arc<clickhouse::error::Error>),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match *self {
            Error::UnableToReadProtobuf(ref err) => write!(f, "Unable to read protobuf: {}", err),
            Error::ProtofishParseError(ref e) => write!(f, "Protofish parse error: {}", e),
            Error::Fdb(ref e) => write!(f, "Fdb error: {}", e),
            Error::Elapsed(ref e) => write!(f, "Tokio timeout elapsed error: {}", e),
            Error::UnableToReadConfig(ref err) => {
                write!(f, "Unable to read configuration: {}", err)
            }
            Error::UnableToWriteConfig(ref err) => {
                write!(f, "Unable to write configuration: {}", err)
            }
            Error::InvalidMappingConfig(ref err) => {
                write!(f, "Invalid mapping configuration: {}", err)
            }
            Error::Clickhouse(ref e) => write!(f, "Clickhouse error: {:?}", e),
        }
    }
}

impl From<FdbError> for Error {
    fn from(err: FdbError) -> Error {
        Error::Fdb(err)
    }
}

impl From<protofish::context::ParseError> for Error {
    fn from(err: protofish::context::ParseError) -> Error {
        Error::ProtofishParseError(err)
    }
}

impl From<tokio::time::error::Elapsed> for Error {
    fn from(err: tokio::time::error::Elapsed) -> Error {
        Error::Elapsed(err)
    }
}

impl From<clickhouse::error::Error> for Error {
    fn from(err: clickhouse::error::Error) -> Error {
        Error::Clickhouse(Arc::new(err))
    }
}