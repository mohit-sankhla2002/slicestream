//! Generate a synthetic CT-like series so you can try slicestream without real
//! patient data.
//!
//! ```sh
//! cargo run -p slicestream-core --example make_phantom -- data/phantom 120
//! ```

use std::path::Path;

use dicom_core::{DataElement, PrimitiveValue, VR};
use dicom_dictionary_std::{tags, uids};
use dicom_object::{FileMetaTableBuilder, InMemDicomObject};

pub fn write_phantom(
    dir: &Path,
    slices: usize,
    size: u16,
) -> Result<(), Box<dyn std::error::Error>> {
    std::fs::create_dir_all(dir)?;
    let series_uid = "2.25.1000000000000000000000000000001";
    let study_uid = "2.25.1000000000000000000000000000002";
    let n = size as i32;

    for z in 0..slices {
        // A sphere whose cross-section grows and shrinks across the stack,
        // plus a slice-number gradient so ordering mistakes are obvious.
        let zc = z as f64 / slices.max(1) as f64 - 0.5;
        let radius = (0.16 - zc * zc).max(0.0).sqrt() * n as f64;
        let mut pixels = Vec::with_capacity((n * n * 2) as usize);
        for y in 0..n {
            for x in 0..n {
                let dx = (x - n / 2) as f64;
                let dy = (y - n / 2) as f64;
                let inside = dx * dx + dy * dy <= radius * radius;
                let hu: i32 = if inside {
                    1024 + 60
                } else {
                    1024 - 1000 + (x * 200 / n)
                };
                pixels.extend_from_slice(&(hu as u16).to_le_bytes());
            }
        }

        let sop_uid = format!("2.25.1000000000000000000000000001{z:05}");
        let ds = |s: String| PrimitiveValue::from(s);
        let obj = InMemDicomObject::from_element_iter([
            DataElement::new(
                tags::SOP_CLASS_UID,
                VR::UI,
                PrimitiveValue::from(uids::CT_IMAGE_STORAGE),
            ),
            DataElement::new(
                tags::SOP_INSTANCE_UID,
                VR::UI,
                PrimitiveValue::from(sop_uid.as_str()),
            ),
            DataElement::new(tags::MODALITY, VR::CS, PrimitiveValue::from("CT")),
            DataElement::new(
                tags::SERIES_DESCRIPTION,
                VR::LO,
                PrimitiveValue::from("Synthetic phantom"),
            ),
            DataElement::new(
                tags::PATIENT_NAME,
                VR::PN,
                PrimitiveValue::from("PHANTOM^SYNTHETIC"),
            ),
            DataElement::new(
                tags::STUDY_INSTANCE_UID,
                VR::UI,
                PrimitiveValue::from(study_uid),
            ),
            DataElement::new(
                tags::SERIES_INSTANCE_UID,
                VR::UI,
                PrimitiveValue::from(series_uid),
            ),
            DataElement::new(tags::INSTANCE_NUMBER, VR::IS, ds(format!("{}", z + 1))),
            DataElement::new(
                tags::IMAGE_POSITION_PATIENT,
                VR::DS,
                ds(format!("0\\0\\{}", z as f64 * 1.25)),
            ),
            DataElement::new(
                tags::IMAGE_ORIENTATION_PATIENT,
                VR::DS,
                ds("1\\0\\0\\0\\1\\0".into()),
            ),
            DataElement::new(tags::SAMPLES_PER_PIXEL, VR::US, PrimitiveValue::from(1_u16)),
            DataElement::new(
                tags::PHOTOMETRIC_INTERPRETATION,
                VR::CS,
                PrimitiveValue::from("MONOCHROME2"),
            ),
            DataElement::new(tags::ROWS, VR::US, PrimitiveValue::from(size)),
            DataElement::new(tags::COLUMNS, VR::US, PrimitiveValue::from(size)),
            DataElement::new(tags::BITS_ALLOCATED, VR::US, PrimitiveValue::from(16_u16)),
            DataElement::new(tags::BITS_STORED, VR::US, PrimitiveValue::from(12_u16)),
            DataElement::new(tags::HIGH_BIT, VR::US, PrimitiveValue::from(11_u16)),
            DataElement::new(
                tags::PIXEL_REPRESENTATION,
                VR::US,
                PrimitiveValue::from(0_u16),
            ),
            DataElement::new(tags::RESCALE_INTERCEPT, VR::DS, ds("-1024".into())),
            DataElement::new(tags::RESCALE_SLOPE, VR::DS, ds("1".into())),
            DataElement::new(tags::WINDOW_CENTER, VR::DS, ds("40".into())),
            DataElement::new(tags::WINDOW_WIDTH, VR::DS, ds("400".into())),
            DataElement::new(tags::PIXEL_DATA, VR::OW, PrimitiveValue::from(pixels)),
        ]);
        let file = obj.with_meta(
            FileMetaTableBuilder::new()
                .transfer_syntax(uids::EXPLICIT_VR_LITTLE_ENDIAN)
                .media_storage_sop_class_uid(uids::CT_IMAGE_STORAGE),
        )?;
        // Write in reverse-ish order on disk to prove sorting works.
        file.write_to_file(dir.join(format!("IM{:05}.dcm", slices - z)))?;
    }
    Ok(())
}

#[allow(dead_code)]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let dir = args.next().unwrap_or_else(|| "data/phantom".into());
    let slices: usize = args.next().map(|s| s.parse()).transpose()?.unwrap_or(120);
    write_phantom(Path::new(&dir), slices, 256)?;
    println!("wrote {slices} slices to {dir}");
    Ok(())
}
