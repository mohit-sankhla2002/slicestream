//! DIMSE command set parsing and response construction (Implicit VR LE).

use dicom_core::{DataElement, VR, dicom_value};
use dicom_dictionary_std::tags;
use dicom_object::InMemDicomObject;
use dicom_transfer_syntax_registry::entries::IMPLICIT_VR_LITTLE_ENDIAN;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DimseRequest {
    Echo {
        message_id: u16,
        sop_class_uid: String,
    },
    Store {
        message_id: u16,
        sop_class_uid: String,
        sop_instance_uid: String,
        has_dataset: bool,
    },
    Other {
        command_field: u16,
        message_id: u16,
        sop_class_uid: Option<String>,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum CommandError {
    #[error("malformed command set: {0}")]
    Parse(String),
    #[error("missing or invalid command element {0}")]
    Missing(&'static str),
}

fn uid_of(obj: &InMemDicomObject, tag: dicom_core::Tag) -> Option<String> {
    let s = obj.element(tag).ok()?.to_str().ok()?;
    Some(s.trim_end_matches(['\0', ' ']).to_string())
}

fn u16_of(
    obj: &InMemDicomObject,
    tag: dicom_core::Tag,
    name: &'static str,
) -> Result<u16, CommandError> {
    obj.element(tag)
        .map_err(|_| CommandError::Missing(name))?
        .to_int::<u16>()
        .map_err(|_| CommandError::Missing(name))
}

pub fn parse_command(bytes: &[u8]) -> Result<DimseRequest, CommandError> {
    let obj = InMemDicomObject::read_dataset_with_ts(bytes, &IMPLICIT_VR_LITTLE_ENDIAN.erased())
        .map_err(|e| CommandError::Parse(e.to_string()))?;
    let command_field = u16_of(&obj, tags::COMMAND_FIELD, "CommandField")?;
    let message_id = u16_of(&obj, tags::MESSAGE_ID, "MessageID")?;
    let sop_class_uid = uid_of(&obj, tags::AFFECTED_SOP_CLASS_UID);
    match command_field {
        0x0030 => Ok(DimseRequest::Echo {
            message_id,
            sop_class_uid: sop_class_uid.ok_or(CommandError::Missing("AffectedSOPClassUID"))?,
        }),
        0x0001 => {
            let ds = u16_of(&obj, tags::COMMAND_DATA_SET_TYPE, "CommandDataSetType")?;
            Ok(DimseRequest::Store {
                message_id,
                sop_class_uid: sop_class_uid.ok_or(CommandError::Missing("AffectedSOPClassUID"))?,
                sop_instance_uid: uid_of(&obj, tags::AFFECTED_SOP_INSTANCE_UID)
                    .ok_or(CommandError::Missing("AffectedSOPInstanceUID"))?,
                has_dataset: ds != 0x0101,
            })
        }
        _ => Ok(DimseRequest::Other {
            command_field,
            message_id,
            sop_class_uid,
        }),
    }
}

fn serialize(obj: InMemDicomObject) -> Vec<u8> {
    let mut buf = Vec::new();
    obj.write_dataset_with_ts(&mut buf, &IMPLICIT_VR_LITTLE_ENDIAN.erased())
        .expect("writing to a Vec cannot fail");
    buf
}

fn us(tag: dicom_core::Tag, v: u16) -> DataElement<InMemDicomObject> {
    DataElement::new(tag, VR::US, dicom_value!(U16, [v]))
}

fn ui(tag: dicom_core::Tag, v: &str) -> DataElement<InMemDicomObject> {
    DataElement::new(tag, VR::UI, dicom_value!(Str, v))
}

pub fn echo_rsp(message_id: u16, sop_class_uid: &str) -> Vec<u8> {
    failure_rsp(0x0030, message_id, Some(sop_class_uid), 0x0000)
}

pub fn store_rsp(
    message_id: u16,
    sop_class_uid: &str,
    sop_instance_uid: &str,
    status: u16,
) -> Vec<u8> {
    serialize(InMemDicomObject::command_from_element_iter([
        ui(tags::AFFECTED_SOP_CLASS_UID, sop_class_uid),
        us(tags::COMMAND_FIELD, 0x8001),
        us(tags::MESSAGE_ID_BEING_RESPONDED_TO, message_id),
        us(tags::COMMAND_DATA_SET_TYPE, 0x0101),
        us(tags::STATUS, status),
        ui(tags::AFFECTED_SOP_INSTANCE_UID, sop_instance_uid),
    ]))
}

pub fn failure_rsp(
    command_field: u16,
    message_id: u16,
    sop_class_uid: Option<&str>,
    status: u16,
) -> Vec<u8> {
    let mut elems = Vec::new();
    if let Some(uid) = sop_class_uid {
        elems.push(ui(tags::AFFECTED_SOP_CLASS_UID, uid));
    }
    elems.push(us(tags::COMMAND_FIELD, command_field | 0x8000));
    elems.push(us(tags::MESSAGE_ID_BEING_RESPONDED_TO, message_id));
    elems.push(us(tags::COMMAND_DATA_SET_TYPE, 0x0101));
    elems.push(us(tags::STATUS, status));
    serialize(InMemDicomObject::command_from_element_iter(elems))
}

#[cfg(test)]
pub(crate) fn test_store_rq(message_id: u16) -> Vec<u8> {
    serialize(InMemDicomObject::command_from_element_iter([
        ui(tags::AFFECTED_SOP_CLASS_UID, "1.2.840.10008.5.1.4.1.1.4"),
        us(tags::COMMAND_FIELD, 0x0001),
        us(tags::MESSAGE_ID, message_id),
        us(tags::PRIORITY, 0),
        us(tags::COMMAND_DATA_SET_TYPE, 0x0102),
        ui(tags::AFFECTED_SOP_INSTANCE_UID, "1.2.3.4"),
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read(bytes: &[u8]) -> InMemDicomObject {
        InMemDicomObject::read_dataset_with_ts(bytes, &IMPLICIT_VR_LITTLE_ENDIAN.erased()).unwrap()
    }

    fn get(o: &InMemDicomObject, t: dicom_core::Tag) -> u16 {
        o.element(t).unwrap().to_int::<u16>().unwrap()
    }

    #[test]
    fn echo_rsp_parses_back() {
        let o = read(&echo_rsp(5, "1.2.840.10008.1.1"));
        assert_eq!(get(&o, tags::COMMAND_FIELD), 0x8030);
        assert_eq!(get(&o, tags::STATUS), 0);
        assert_eq!(get(&o, tags::MESSAGE_ID_BEING_RESPONDED_TO), 5);
    }

    #[test]
    fn failure_rsp_sets_response_bit() {
        let o = read(&failure_rsp(0x0020, 3, None, 0x0211));
        assert_eq!(get(&o, tags::COMMAND_FIELD), 0x8020);
        assert_eq!(get(&o, tags::STATUS), 0x0211);
    }

    #[test]
    fn store_request_parses() {
        match parse_command(&test_store_rq(9)).unwrap() {
            DimseRequest::Store {
                sop_instance_uid,
                sop_class_uid,
                ..
            } => {
                assert_eq!(sop_instance_uid, "1.2.3.4");
                assert_eq!(sop_class_uid, "1.2.840.10008.5.1.4.1.1.4");
            }
            other => panic!("{other:?}"),
        }
    }
}
