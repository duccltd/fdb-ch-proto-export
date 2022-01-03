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
    ParseError(String),
    StringDecodeError(std::string::FromUtf8Error),
    NoAvailableColumnBinding(String),
    NoProtoDefault(String),
    MissingConfig(String),
    UnknownValueType,
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
            Error::ParseError(ref e) => write!(f, "Unable to parse: {:?}", e),
            Error::StringDecodeError(ref e) => write!(f, "String decode error: {}", e),
            Error::NoAvailableColumnBinding(ref e) => {
                write!(f, "No column binding available for column: {:?}", e)
            }
            Error::NoProtoDefault(ref e) => {
                write!(f, "Could not find field or produce default: {:?}", e)
            }
            Error::MissingConfig(ref e) => write!(f, "Could not find config: {:?}", e),
            Error::UnknownValueType => write!(f, "Unknown value type"),
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

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::UnableToReadConfig(err)
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::ParseError(format!(
            "Could not deserialize file due to invalid format: {:?}",
            err
        ))
    }
}
