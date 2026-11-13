//! Fixture builders for the walker tests. Everything is synthesized; no real data.
#![allow(dead_code)]

use std::io::{Cursor, Write};

use flate2::Compression;
use flate2::write::GzEncoder;
use zip::write::{SimpleFileOptions, ZipWriter};

/// Minimal Part-10 bytes: 128-byte preamble, `DICM`, then `tag` bytes so
/// different fixtures hash differently.
pub fn dicom(tag: &str) -> Vec<u8> {
    let mut v = vec![0u8; 128];
    v.extend_from_slice(b"DICM");
    v.extend_from_slice(&[0x02, 0x00, 0x10, 0x00, b'U', b'I']);
    v.extend_from_slice(tag.as_bytes());
    v
}

/// A DICOMDIR: Part-10 with MediaStorageSOPClassUID = Media Storage Directory.
pub fn dicomdir() -> Vec<u8> {
    let mut v = vec![0u8; 128];
    v.extend_from_slice(b"DICM");
    let uid = b"1.2.840.10008.1.3.10";
    v.extend_from_slice(&[0x02, 0x00, 0x02, 0x00, b'U', b'I', uid.len() as u8, 0]);
    v.extend_from_slice(uid);
    v.extend_from_slice(&[0x02, 0x00, 0x10, 0x00, b'U', b'I', 2, 0, b'1', 0]);
    v
}

/// Tiny NIfTI-1 single file (`.nii`): 348-byte header, 4-byte extension, data.
pub fn nifti1() -> Vec<u8> {
    let mut v = vec![0u8; 352];
    v[0..4].copy_from_slice(&348i32.to_le_bytes());
    // dim: 3 dims of 2x2x2
    let dims: [i16; 8] = [3, 2, 2, 2, 1, 1, 1, 1];
    for (i, d) in dims.iter().enumerate() {
        v[40 + i * 2..42 + i * 2].copy_from_slice(&d.to_le_bytes());
    }
    v[70..72].copy_from_slice(&2i16.to_le_bytes()); // datatype uint8
    v[72..74].copy_from_slice(&8i16.to_le_bytes()); // bitpix
    v[108..112].copy_from_slice(&352f32.to_le_bytes()); // vox_offset
    v[344..348].copy_from_slice(b"n+1\0");
    v.extend_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8]);
    v
}

pub fn gz(bytes: &[u8]) -> Vec<u8> {
    let mut e = GzEncoder::new(Vec::new(), Compression::default());
    e.write_all(bytes).unwrap();
    e.finish().unwrap()
}

/// ZIP with deflated entries. A name ending in `/` becomes a directory entry.
pub fn zip(entries: &[(&str, &[u8])]) -> Vec<u8> {
    let mut w = ZipWriter::new(Cursor::new(Vec::new()));
    let opts = SimpleFileOptions::default().compression_method(zip::CompressionMethod::Deflated);
    for (name, data) in entries {
        if name.ends_with('/') {
            w.add_directory(*name, opts).unwrap();
        } else {
            w.start_file(*name, opts).unwrap();
            w.write_all(data).unwrap();
        }
    }
    w.finish().unwrap().into_inner()
}

/// ZIP with stored (uncompressed) entries, so content bytes appear verbatim.
pub fn zip_stored(entries: &[(&str, &[u8])]) -> Vec<u8> {
    let mut w = ZipWriter::new(Cursor::new(Vec::new()));
    let opts = SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    for (name, data) in entries {
        w.start_file(*name, opts).unwrap();
        w.write_all(data).unwrap();
    }
    w.finish().unwrap().into_inner()
}

/// Flip a byte of the first occurrence of `needle` (corrupts stored data).
pub fn corrupt(mut z: Vec<u8>, needle: &[u8]) -> Vec<u8> {
    let i = z.windows(needle.len()).position(|w| w == needle).unwrap();
    z[i + needle.len() - 1] ^= 0xFF;
    z
}

/// Set the encrypted flag on the local and central headers of entry `name`.
pub fn mark_entry_encrypted(mut z: Vec<u8>, name: &str) -> Vec<u8> {
    let n = name.as_bytes();
    let mut i = 0;
    while i + 46 + n.len() <= z.len() {
        if &z[i..i + 4] == b"PK\x03\x04" && &z[i + 30..i + 30 + n.len()] == n {
            z[i + 6] |= 1;
        } else if &z[i..i + 4] == b"PK\x01\x02" && &z[i + 46..i + 46 + n.len()] == n {
            z[i + 8] |= 1;
        }
        i += 1;
    }
    z
}

/// Overwrite the end-of-central-directory record's directory size field.
pub fn set_cd_size(mut z: Vec<u8>, size: u32) -> Vec<u8> {
    let i = z.windows(4).rposition(|w| w == b"PK\x05\x06").unwrap();
    z[i + 12..i + 16].copy_from_slice(&size.to_le_bytes());
    z
}

/// A raw tar header block plus `extra` bytes, then the end-of-archive marker.
pub fn tar_raw_header(h: &tar::Header, extra: &[u8]) -> Vec<u8> {
    let mut v = h.as_bytes().to_vec();
    v.extend_from_slice(extra);
    let pad = (512 - v.len() % 512) % 512;
    v.extend(std::iter::repeat_n(0u8, pad + 1024));
    v
}

/// Set the "encrypted" general-purpose flag on every local and central header.
pub fn mark_encrypted(mut z: Vec<u8>) -> Vec<u8> {
    let mut i = 0;
    while i + 4 <= z.len() {
        if &z[i..i + 4] == b"PK\x03\x04" {
            z[i + 6] |= 1;
        } else if &z[i..i + 4] == b"PK\x01\x02" {
            z[i + 8] |= 1;
        }
        i += 1;
    }
    z
}

pub enum TarItem<'a> {
    File(&'a str, &'a [u8]),
    /// File preceded by a pax header carrying `path`.
    PaxFile(&'a str, &'a [u8]),
    Dir(&'a str),
    Symlink(&'a str, &'a str),
}

pub fn tar(items: &[TarItem<'_>]) -> Vec<u8> {
    let mut b = tar::Builder::new(Vec::new());
    for it in items {
        let mut h = tar::Header::new_ustar();
        match it {
            TarItem::File(name, data) => {
                h.set_entry_type(tar::EntryType::Regular);
                h.set_size(data.len() as u64);
                h.set_mode(0o644);
                h.set_cksum();
                b.append_data(&mut h, name, *data).unwrap();
            }
            TarItem::PaxFile(name, data) => {
                b.append_pax_extensions([("path", name.as_bytes())])
                    .unwrap();
                h.set_entry_type(tar::EntryType::Regular);
                h.set_size(data.len() as u64);
                h.set_mode(0o644);
                h.set_path("short-ignored").unwrap();
                h.set_cksum();
                b.append(&h, *data).unwrap();
            }
            TarItem::Dir(name) => {
                h.set_entry_type(tar::EntryType::Directory);
                h.set_size(0);
                h.set_mode(0o755);
                h.set_cksum();
                b.append_data(&mut h, name, std::io::empty()).unwrap();
            }
            TarItem::Symlink(name, target) => {
                h.set_entry_type(tar::EntryType::Symlink);
                h.set_size(0);
                h.set_mode(0o777);
                b.append_link(&mut h, name, target).unwrap();
            }
        }
    }
    b.into_inner().unwrap()
}
