// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;

use anyhow::{anyhow, bail, Context};

use protobuf::descriptor::FileDescriptorSet;
use protobuf::{CodedInputStream, Message as _};
use serde_protobuf::descriptor::{
    Descriptors, FieldDescriptor, FieldLabel, FieldType, MessageDescriptor,
};
use serde_protobuf::value::{Field, Message, Value};

use ore::str::StrExt;
use repr::{ColumnName, ColumnType, Datum, Row, ScalarType};

/// A decoded description of the schema of a Protobuf message.
#[derive(Debug)]
pub struct DecodedDescriptors {
    descriptors: Descriptors,
    message_name: String,
    columns: Vec<(ColumnName, ColumnType)>,
}

impl DecodedDescriptors {
    /// Builds a `DecodedDescriptors` from an encoded [`FileDescriptorSet`]
    /// and the fully qualified name of a message inside that file descriptor
    /// set.
    pub fn from_bytes(bytes: &[u8], message_name: String) -> Result<Self, anyhow::Error> {
        let fds =
            FileDescriptorSet::parse_from_bytes(bytes).context("parsing file descriptor set")?;
        let descriptors = Descriptors::from_proto(&fds);

        let message = descriptors.message_by_name(&message_name).ok_or_else(|| {
            // TODO(benesch): the error message here used to include the names of
            // all messages in the descriptor set, but that one feature required
            // maintaining a fork of serde_protobuf. I sent the patch upstream [0],
            // and we can add the error message improvement back if that patch is
            // accepted.
            // [0]: https://github.com/dflemstr/serde-protobuf/pull/9
            anyhow!(
                "protobuf message {} not found in file descriptor set",
                message_name.quoted()
            )
        })?;
        let mut seen_messages = HashSet::new();
        seen_messages.insert(message.name());
        let mut columns = vec![];
        for field in message.fields() {
            let name = ColumnName::from(field.name());
            let ty = derive_column_type(&mut seen_messages, &field, &descriptors)?;
            columns.push((name, ty))
        }

        Ok(DecodedDescriptors {
            descriptors,
            message_name,
            columns,
        })
    }

    /// Describes the columns in the message.
    ///
    /// In other words, the return value describes the shape of the rows that
    /// will be produced by a [`Decoder`] constructed from this
    /// `DecodedDescriptors`.
    pub fn columns(&self) -> &[(ColumnName, ColumnType)] {
        &self.columns
    }

    fn message_descriptor(&self) -> &MessageDescriptor {
        self.descriptors
            .message_by_name(&self.message_name)
            .expect("message validated to exist")
    }
}

/// Decodes a particular Protobuf message from its wire format.
#[derive(Debug)]
pub struct Decoder {
    descriptors: DecodedDescriptors,
    packer: Row,
}

impl Decoder {
    /// Constructs a decoder for a particular Protobuf message.
    pub fn new(descriptors: DecodedDescriptors) -> Self {
        Decoder {
            descriptors,
            packer: Row::default(),
        }
    }

    /// Decodes the encoded Protobuf message into a [`Row`].
    pub fn decode(&mut self, bytes: &[u8]) -> Result<Option<Row>, anyhow::Error> {
        let message_desc = self.descriptors.message_descriptor();
        let mut input_stream = CodedInputStream::from_bytes(bytes);
        let mut message = Message::new(message_desc);
        message.merge_from(
            &self.descriptors.descriptors,
            message_desc,
            &mut input_stream,
        )?;
        pack_message(
            &mut self.packer,
            &self.descriptors.descriptors,
            message_desc,
            message,
        )?;
        Ok(Some(self.packer.finish_and_reuse()))
    }
}

fn derive_column_type<'a>(
    seen_messages: &mut HashSet<&'a str>,
    field: &'a FieldDescriptor,
    descriptors: &'a Descriptors,
) -> Result<ColumnType, anyhow::Error> {
    let field_type = field.field_type(descriptors);
    let scalar_type = match field_type {
        FieldType::Bool => ScalarType::Bool,
        FieldType::Int32 | FieldType::SInt32 | FieldType::SFixed32 => ScalarType::Int32,
        FieldType::Int64 | FieldType::SInt64 | FieldType::SFixed64 => ScalarType::Int64,
        FieldType::Enum(_) => ScalarType::String,
        FieldType::Float => ScalarType::Float32,
        FieldType::Double => ScalarType::Float64,
        FieldType::UInt32 => bail!("Protobuf type \"uint32\" is not supported"),
        FieldType::UInt64 => bail!("Protobuf type \"uint64\" is not supported"),
        FieldType::Fixed32 => bail!("Protobuf type \"fixed32\" is not supported"),
        FieldType::Fixed64 => bail!("Protobuf type \"fixed64\" is not supported"),
        FieldType::String => ScalarType::String,
        FieldType::Bytes => ScalarType::Bytes,
        FieldType::Message(m) => {
            if seen_messages.contains(m.name()) {
                bail!("Recursive types are not supported: {}", m.name());
            }
            seen_messages.insert(m.name());
            let mut fields = Vec::with_capacity(m.fields().len());
            for field in m.fields() {
                let column_name = ColumnName::from(field.name());
                let column_type = derive_column_type(seen_messages, field, descriptors)?;
                fields.push((column_name, column_type))
            }
            seen_messages.remove(m.name());
            ScalarType::Record {
                fields,
                custom_oid: None,
                custom_name: None,
            }
        }
        FieldType::Group => bail!("Unions are currently not supported"),
        FieldType::UnresolvedMessage(m) => bail!("Unresolved message {} not supported", m),
        FieldType::UnresolvedEnum(e) => bail!("Unresolved enum {} not supported", e),
    };

    match field.field_label() {
        FieldLabel::Required => bail!("Required field {} not supported", field.name()),
        FieldLabel::Repeated => Ok(ColumnType {
            nullable: false,
            scalar_type: ScalarType::List {
                element_type: Box::new(scalar_type),
                custom_oid: None,
            },
        }),
        FieldLabel::Optional => Ok(ColumnType {
            nullable: matches!(field_type, FieldType::Message(_)),
            scalar_type,
        }),
    }
}

fn pack_message(
    packer: &mut Row,
    descriptors: &Descriptors,
    message_desc: &MessageDescriptor,
    message: Message,
) -> Result<(), anyhow::Error> {
    for (field_desc, (_id, field)) in message_desc.fields().iter().zip(message.fields) {
        pack_field(packer, descriptors, field_desc, field)?;
    }
    Ok(())
}

fn pack_field(
    packer: &mut Row,
    descriptors: &Descriptors,
    field_desc: &FieldDescriptor,
    field: Field,
) -> Result<(), anyhow::Error> {
    match field {
        Field::Singular(None) => pack_default_value(packer, descriptors, field_desc),
        Field::Singular(Some(value)) => pack_value(packer, descriptors, field_desc, value),
        Field::Repeated(values) => packer.push_list_with(|packer| {
            for value in values {
                pack_value(packer, descriptors, field_desc, value)?;
            }
            Ok::<_, anyhow::Error>(())
        }),
    }
}

fn pack_value(
    packer: &mut Row,
    descriptors: &Descriptors,
    field_desc: &FieldDescriptor,
    value: Value,
) -> Result<(), anyhow::Error> {
    match value {
        Value::Bool(false) => packer.push(Datum::False),
        Value::Bool(true) => packer.push(Datum::True),
        Value::I32(i) => packer.push(Datum::Int32(i)),
        Value::I64(i) => packer.push(Datum::Int64(i)),
        Value::F32(f) => packer.push(Datum::Float32(f.into())),
        Value::F64(f) => packer.push(Datum::Float64(f.into())),
        Value::String(s) => packer.push(Datum::String(&s)),
        Value::Bytes(b) => packer.push(Datum::Bytes(&b)),
        Value::Enum(e) => {
            let enum_desc = match field_desc.field_type(descriptors) {
                FieldType::Enum(enum_desc) => enum_desc,
                ty => bail!(
                    "internal error: protobuf enum value has unexpected type {:?}",
                    ty
                ),
            };
            match enum_desc.value_by_number(e) {
                None => {
                    bail!(
                        "error decoding protobuf: enum value {} is missing while decoding field {}",
                        e,
                        field_desc.name()
                    );
                }
                Some(ev) => packer.push(Datum::String(ev.name())),
            }
        }
        Value::Message(m) => {
            let message_desc = match field_desc.field_type(descriptors) {
                FieldType::Message(message_desc) => message_desc,
                ty => bail!(
                    "internal error: protobuf message value has unexpected type {:?}",
                    ty
                ),
            };
            packer.push_list_with(|packer| pack_message(packer, descriptors, message_desc, m))?;
        }
        Value::U32(_) | Value::U64(_) => {
            bail!(
                "internal error: unexpected value while decoding protobuf message: {:?}",
                value
            );
        }
    };
    Ok(())
}

fn pack_default_value(
    packer: &mut Row,
    descriptors: &Descriptors,
    field_desc: &FieldDescriptor,
) -> Result<(), anyhow::Error> {
    if let Some(default) = field_desc.default_value() {
        return pack_value(packer, descriptors, field_desc, default.clone());
    }
    match field_desc.field_type(descriptors) {
        FieldType::Bool => packer.push(Datum::False),
        FieldType::Int32 | FieldType::SInt32 | FieldType::SFixed32 => packer.push(Datum::Int32(0)),
        FieldType::Int64 | FieldType::SInt64 | FieldType::SFixed64 => packer.push(Datum::Int64(0)),
        FieldType::Float => packer.push(Datum::Float32(0.0.into())),
        FieldType::Double => packer.push(Datum::Float64(0.0.into())),
        FieldType::String => packer.push(Datum::String("")),
        FieldType::Bytes => packer.push(Datum::Bytes(&[])),
        FieldType::Message(_) => packer.push(Datum::Null),
        FieldType::Enum(e) => match e.value_by_number(0) {
            None => bail!(
                "error decoding protobuf: enum value 0 is missing while decoding field {}",
                field_desc.name()
            ),
            Some(ev) => packer.push(Datum::String(ev.name())),
        },
        ty @ FieldType::UInt32
        | ty @ FieldType::UInt64
        | ty @ FieldType::Fixed32
        | ty @ FieldType::Fixed64
        | ty @ FieldType::Group
        | ty @ FieldType::UnresolvedMessage(_)
        | ty @ FieldType::UnresolvedEnum(_) => {
            bail!(
                "internal error: unexpected type while decoding protobuf message: {:?}",
                ty
            );
        }
    }
    Ok(())
}
