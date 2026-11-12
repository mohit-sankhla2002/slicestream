//! Part-10 preamble + file meta group for datasets received without one (DIMSE).

use std::io;

use dicom_object::FileMetaTableBuilder;

use crate::{SpoolError, TempBlob};

/// Implementation Class UID written into every file meta group we create.
pub const IMPLEMENTATION_CLASS_UID: &str = "2.25.74886492046038186129868855052748391807";
/// Implementation Version Name written into every file meta group we create.
pub const IMPLEMENTATION_VERSION_NAME: &str = "SLICESTREAM_010";

/// Write the 128-byte zero preamble, `DICM` and a file meta group to `temp`.
/// The dataset bytes (in `transfer_syntax`) must follow.
pub async fn write_part10_prefix(
    temp: &mut TempBlob,
    sop_class_uid: &str,
    sop_instance_uid: &str,
    transfer_syntax: &str,
    source_ae: Option<&str>,
) -> Result<(), SpoolError> {
    let mut builder = FileMetaTableBuilder::new()
        .media_storage_sop_class_uid(sop_class_uid)
        .media_storage_sop_instance_uid(sop_instance_uid)
        .transfer_syntax(transfer_syntax)
        .implementation_class_uid(IMPLEMENTATION_CLASS_UID)
        .implementation_version_name(IMPLEMENTATION_VERSION_NAME);
    if let Some(ae) = source_ae {
        builder = builder.source_application_entity_title(ae);
    }
    let meta = builder.build().map_err(io::Error::other)?;

    let mut buf = Vec::with_capacity(132 + 256);
    buf.extend_from_slice(&[0u8; 128]);
    buf.extend_from_slice(b"DICM");
    // FileMetaTable::write emits only the group-0002 elements.
    meta.write(&mut buf).map_err(io::Error::other)?;
    temp.write_chunk(&buf).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FsBlobStore;
    use dicom_dictionary_std::uids;
    use dicom_object::open_file;
    use dicom_transfer_syntax_registry::entries::EXPLICIT_VR_LITTLE_ENDIAN;

    #[tokio::test]
    async fn part10_prefix_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        crate::phantom::write_phantom(&dir.path().join("src"), 1, 16).unwrap();
        let obj = open_file(dir.path().join("src/IM00001.dcm")).unwrap();
        let sop = obj.meta().media_storage_sop_instance_uid().to_string();

        let store = FsBlobStore::open(&dir.path().join("store")).unwrap();
        let mut temp = store.create_temp(1 << 20).unwrap();
        write_part10_prefix(
            &mut temp,
            uids::CT_IMAGE_STORAGE,
            &sop,
            uids::EXPLICIT_VR_LITTLE_ENDIAN,
            Some("MODALITY1"),
        )
        .await
        .unwrap();
        let mut body = Vec::new();
        obj.write_dataset_with_ts(&mut body, &EXPLICIT_VR_LITTLE_ENDIAN.erased())
            .unwrap();
        temp.write_chunk(&body).await.unwrap();
        let spooled = temp.finish().await.unwrap();

        let bytes = std::fs::read(spooled.path()).unwrap();
        assert!(bytes[..128].iter().all(|b| *b == 0));
        assert_eq!(&bytes[128..132], b"DICM");

        let h = crate::read_header(spooled.path()).unwrap();
        assert_eq!(h.transfer_syntax, uids::EXPLICIT_VR_LITTLE_ENDIAN);
        assert_eq!(h.sop_instance_uid, sop);
        assert_eq!(h.frame_info.unwrap().rows, 16);

        let full = open_file(spooled.path()).unwrap();
        assert_eq!(
            full.meta().implementation_class_uid(),
            "2.25.74886492046038186129868855052748391807"
        );
        assert_eq!(
            full.meta().implementation_version_name(),
            Some("SLICESTREAM_010")
        );
        assert_eq!(
            full.meta().source_application_entity_title(),
            Some("MODALITY1")
        );
    }
}
