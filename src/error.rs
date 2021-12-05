use std::fmt::Formatter;

#[derive(Debug)]
pub enum Error {
    UnableToReadProtobuf(std::io::Error),
    ProtofishParseError(protofish::context::ParseError),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match *self {
            Error::UnableToReadProtobuf(ref err) => write!(f, "Unable to read protobuf: {}", err),
            Error::ProtofishParseError(ref e) => write!(f, "Protofish parse error: {}", e),
        }
    }
}

impl From<protofish::context::ParseError> for Error {
    fn from(err: protofish::context::ParseError) -> Error {
        Error::ProtofishParseError(err)
    }
}