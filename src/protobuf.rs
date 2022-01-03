use serde::Serialize;
use tracing::error;

use std::collections::HashMap;

use crate::error::Error;
use crate::result::Result;
use protofish::{context::Context, prelude::Value};
use std::path::Path;

pub async fn load_protobufs(path: impl AsRef<Path>) -> Result<Context> {
    let common_types = get_common_types().await?;

    let protos = tokio::fs::read_to_string(path)
        .await
        .map_err(Error::UnableToReadProtobuf)?;

    Ok(Context::parse(&[common_types, vec![protos]].concat())?)
}

async fn get_common_types() -> Result<Vec<String>> {
    let mut bufs = vec![];
    let mut dir = tokio::fs::read_dir("google_protobuf")
        .await
        .map_err(Error::UnableToReadProtobuf)?;
    while let Some(entry) = dir
        .next_entry()
        .await
        .map_err(Error::UnableToReadProtobuf)?
    {
        if !entry
            .file_name()
            .to_str()
            .expect("common types proto file name cannot be converted to &str")
            .contains(".proto")
        {
            continue;
        }

        let contents = tokio::fs::read_to_string(entry.path())
            .await
            .expect("unable to read file");

        bufs.push(contents);
    }

    Ok(bufs)
}

#[derive(Serialize)]
struct Record {
    key: String,
    fields: HashMap<String, serde_json::Value>,
}

#[derive(Serialize)]
struct Field {
    name: String,
    value: serde_json::Value,
}

pub fn value_to_string(
    context: &protofish::context::Context,
    value: &protofish::prelude::Value,
) -> Result<String> {
    use protofish::prelude::Value::*;

    Ok(match value {
        Double(v) => v.to_string(),
        Float(v) => v.to_string(),
        Int32(v) => v.to_string(),
        Int64(v) => v.to_string(),
        UInt32(v) => v.to_string(),
        UInt64(v) => v.to_string(),
        SInt32(v) => v.to_string(),
        SInt64(v) => v.to_string(),
        Fixed32(v) => v.to_string(),
        Fixed64(v) => v.to_string(),
        SFixed32(v) => v.to_string(),
        SFixed64(v) => v.to_string(),
        Bool(v) => v.to_string(),
        String(v) => format!("'{}'", v.replace("'", "\\'")),

        // Packed(v) => match v {
        //     PackedArray::Double(v) => to_value(v)?,
        //     PackedArray::Float(v) => to_value(v)?,
        //     PackedArray::Int32(v) => to_value(v)?,
        //     PackedArray::Int64(v) => to_value(v)?,
        //     PackedArray::UInt32(v) => to_value(v)?,
        //     PackedArray::UInt64(v) => to_value(v)?,
        //     PackedArray::SInt32(v) => to_value(v)?,
        //     PackedArray::SInt64(v) => to_value(v)?,
        //     PackedArray::Fixed32(v) => to_value(v)?,
        //     PackedArray::Fixed64(v) => to_value(v)?,
        //     PackedArray::SFixed32(v) => to_value(v)?,
        //     PackedArray::SFixed64(v) => to_value(v)?,
        //     PackedArray::Bool(v) => to_value(v)?,
        // },

        Enum(v) => {
            let resolved = context.resolve_enum(v.enum_ref);

            format!("'{}'", resolved.get_field_by_value(v.value).unwrap().name.clone())
        }

        Message(v) => {
            let resolved = context.resolve_message(v.msg_ref);

            let mut fields = v.fields.clone().into_iter();

            // Handle google protobuf timestamp
            if resolved.full_name == "google.protobuf.Timestamp" {
                if let Some(seconds) = fields.find(|f| f.number == 1) {
                    if let Value::Int64(v) = seconds.value {
                        return Ok(v.to_string());
                    }
                }
            }
            
            let values: HashMap<std::string::String, std::string::String> = v.fields
                .clone()
                .into_iter()
                .map(|field| {
                    let name = &resolved.get_field(field.number).unwrap().name;

                    (name.clone(), value_to_string(context, &field.value).unwrap())
                })
                .collect();

            let v = serde_json::to_string(&values)?;

            // Sub object will serialize strings with ''
            v.to_string().replace("'", "")
        }

        _ => {
            error!("Unable to convert proto value to string: {:?}", value);
            return Err(Error::ParseError("Unsupported prototype".into()));
        }

        // Value which was incomplete due to missing bytes in the payload.
        // Incomplete(_, v) => serde_json::Value::String(format!(
        //     "INCOMPLETE: {}",
        //     std::string::String::from_utf8_lossy(&v.to_vec()).to_string()
        // )),

        // Value which wasn't defined in the context.
        // Unknown(v) => serde_json::Value::String(format!("UNKNOWN: {:?}", v)),
    })
}
