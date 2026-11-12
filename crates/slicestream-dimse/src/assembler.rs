//! Reassembles command PDVs; passes data PDVs through.

use dicom_ul::pdu::{PDataValue, PDataValueType};

/// Event produced by [`PdvAssembler::push`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PdvEvent {
    Command {
        pc_id: u8,
        bytes: Vec<u8>,
    },
    Data {
        pc_id: u8,
        chunk: Vec<u8>,
        last: bool,
    },
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum AssemblerError {
    #[error("command set exceeds the maximum size")]
    CommandTooLarge,
    #[error("command fragment for a different presentation context while one is pending")]
    InterleavedCommand,
}

#[derive(Debug)]
pub struct PdvAssembler {
    max_command_bytes: usize,
    pending: Option<(u8, Vec<u8>)>,
}

impl PdvAssembler {
    pub fn new(max_command_bytes: usize) -> Self {
        Self {
            max_command_bytes,
            pending: None,
        }
    }

    pub fn push(&mut self, pdv: PDataValue) -> Result<Option<PdvEvent>, AssemblerError> {
        match pdv.value_type {
            PDataValueType::Data => Ok(Some(PdvEvent::Data {
                pc_id: pdv.presentation_context_id,
                chunk: pdv.data,
                last: pdv.is_last,
            })),
            PDataValueType::Command => {
                let pc_id = pdv.presentation_context_id;
                let (pending_id, buf) = self.pending.get_or_insert_with(|| (pc_id, Vec::new()));
                if *pending_id != pc_id {
                    self.pending = None;
                    return Err(AssemblerError::InterleavedCommand);
                }
                if buf.len() + pdv.data.len() > self.max_command_bytes {
                    self.pending = None;
                    return Err(AssemblerError::CommandTooLarge);
                }
                buf.extend_from_slice(&pdv.data);
                if pdv.is_last {
                    let (pc_id, bytes) = self.pending.take().expect("pending set above");
                    Ok(Some(PdvEvent::Command { pc_id, bytes }))
                } else {
                    Ok(None)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::{DimseRequest, parse_command, test_store_rq};

    fn pdv(t: PDataValueType, last: bool, data: Vec<u8>) -> PDataValue {
        PDataValue {
            presentation_context_id: 1,
            value_type: t,
            is_last: last,
            data,
        }
    }

    #[test]
    fn command_split_across_three_pdvs() {
        let bytes = test_store_rq(7);
        let n = bytes.len() / 3;
        let mut a = PdvAssembler::new(64 * 1024);
        assert!(
            a.push(pdv(PDataValueType::Command, false, bytes[..n].to_vec()))
                .unwrap()
                .is_none()
        );
        assert!(
            a.push(pdv(
                PDataValueType::Command,
                false,
                bytes[n..2 * n].to_vec()
            ))
            .unwrap()
            .is_none()
        );
        let ev = a
            .push(pdv(PDataValueType::Command, true, bytes[2 * n..].to_vec()))
            .unwrap()
            .unwrap();
        let PdvEvent::Command { pc_id, bytes: got } = ev else {
            panic!("expected command")
        };
        assert_eq!(pc_id, 1);
        assert_eq!(got, bytes);
        assert!(matches!(
            parse_command(&got).unwrap(),
            DimseRequest::Store {
                message_id: 7,
                has_dataset: true,
                ..
            }
        ));
    }

    #[test]
    fn data_passes_through() {
        let mut a = PdvAssembler::new(1024);
        for last in [false, true] {
            let ev = a
                .push(pdv(PDataValueType::Data, last, vec![1, 2, 3]))
                .unwrap()
                .unwrap();
            assert_eq!(
                ev,
                PdvEvent::Data {
                    pc_id: 1,
                    chunk: vec![1, 2, 3],
                    last
                }
            );
        }
    }

    #[test]
    fn command_too_large() {
        let mut a = PdvAssembler::new(64 * 1024);
        let mut res = Ok(None);
        for _ in 0..70 {
            res = a.push(pdv(PDataValueType::Command, false, vec![0; 1024]));
            if res.is_err() {
                break;
            }
        }
        assert_eq!(res.unwrap_err(), AssemblerError::CommandTooLarge);
    }

    #[test]
    fn interleaved_command_rejected() {
        let mut a = PdvAssembler::new(1024);
        a.push(pdv(PDataValueType::Command, false, vec![0; 4]))
            .unwrap();
        let mut other = pdv(PDataValueType::Command, false, vec![0; 4]);
        other.presentation_context_id = 3;
        assert_eq!(
            a.push(other).unwrap_err(),
            AssemblerError::InterleavedCommand
        );
    }
}
