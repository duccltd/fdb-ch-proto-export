use protofish::{decode::PackedArray};
use serde::{Serialize};

use std::collections::HashMap;

use crate::error::Error;
use crate::result::Result;
use protofish::context::Context;
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

fn value_to_string(
    context: &protofish::context::Context,
    v: protofish::prelude::Value,
) -> serde_json::Result<serde_json::Value> {
    use protofish::prelude::Value::*;
    use serde_json::to_value;

    Ok(match v {
        Double(v) => to_value(v)?,
        Float(v) => to_value(v)?,
        Int32(v) => to_value(v)?,
        Int64(v) => to_value(v)?,
        UInt32(v) => to_value(v)?,
        UInt64(v) => to_value(v)?,
        SInt32(v) => to_value(v)?,
        SInt64(v) => to_value(v)?,
        Fixed32(v) => to_value(v)?,
        Fixed64(v) => to_value(v)?,
        SFixed32(v) => to_value(v)?,
        SFixed64(v) => to_value(v)?,
        Bool(v) => to_value(v)?,
        String(v) => to_value(v)?,
        Bytes(v) => to_value(v.to_vec())?,

        Packed(v) => match v {
            PackedArray::Double(v) => to_value(v)?,
            PackedArray::Float(v) => to_value(v)?,
            PackedArray::Int32(v) => to_value(v)?,
            PackedArray::Int64(v) => to_value(v)?,
            PackedArray::UInt32(v) => to_value(v)?,
            PackedArray::UInt64(v) => to_value(v)?,
            PackedArray::SInt32(v) => to_value(v)?,
            PackedArray::SInt64(v) => to_value(v)?,
            PackedArray::Fixed32(v) => to_value(v)?,
            PackedArray::Fixed64(v) => to_value(v)?,
            PackedArray::SFixed32(v) => to_value(v)?,
            PackedArray::SFixed64(v) => to_value(v)?,
            PackedArray::Bool(v) => to_value(v)?,
        },

        Message(v) => {
            let resolved = context.resolve_message(v.msg_ref);

            serde_json::Value::Object(
                v.fields
                    .into_iter()
                    .map(|field| {
                        let name = &resolved.get_field(field.number).unwrap().name;

                        (name.clone(), value_to_string(context, field.value).unwrap())
                    })
                    .collect(),
            )
        }

        Enum(v) => {
            let resolved = context.resolve_enum(v.enum_ref);

            let name = resolved.get_field_by_value(v.value).unwrap().name.clone();

            serde_json::Value::String(name)
        }

        // Value which was incomplete due to missing bytes in the payload.
        Incomplete(_, v) => serde_json::Value::String(format!(
            "INCOMPLETE: {}",
            std::string::String::from_utf8_lossy(&v.to_vec()).to_string()
        )),

        // Value which wasn't defined in the context.
        Unknown(v) => serde_json::Value::String(format!("UNKNOWN: {:?}", v)),
    })
}