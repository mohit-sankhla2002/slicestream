//! Fixture builders for the walker tests. Everything is synthesized; no real data.
#![allow(dead_code)]

use std::io::{self, Cursor, Write};
use std::path::Path;

use sha2::{Digest, Sha256};
use slicestream_archive::{Event, Kind, Leaf, Limits, Sink, WalkError, WalkStats, walk_file};

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

#[derive(Debug, Clone, PartialEq)]
pub struct Rec {
    pub path: String,
    pub kind: Kind,
    pub compressed: bool,
    pub sha: String,
    pub head_len: usize,
}

#[derive(Default)]
pub struct Recorder {
    pub leaves: Vec<Rec>,
    pub failed: Vec<(String, String)>,
    pub entry_failed: Vec<(String, String)>,
    pub skipped: Vec<String>,
    /// Leaves at these paths are acknowledged without reading their bytes.
    pub unread: Vec<String>,
    /// `Sink::progress` calls.
    pub progress: Vec<(u64, u64)>,
}

impl Sink for Recorder {
    fn leaf(&mut self, leaf: Leaf<'_>) -> io::Result<()> {
        if self.unread.iter().any(|p| p == leaf.path) {
            return Ok(());
        }
        let mut h = Sha256::new();
        let mut buf = [0u8; 8192];
        loop {
            let n = leaf.reader.read(&mut buf)?;
            if n == 0 {
                break;
            }
            h.update(&buf[..n]);
        }
        self.leaves.push(Rec {
            path: leaf.path.to_string(),
            kind: leaf.kind,
            compressed: leaf.compressed,
            sha: hex(&h.finalize()),
            head_len: leaf.head.len(),
        });
        Ok(())
    }

    fn event(&mut self, ev: Event<'_>) {
        match ev {
            Event::ContainerFailed { path, reason } => {
                self.failed.push((path.to_string(), reason));
            }
            Event::EntryFailed { path, reason } => {
                self.entry_failed.push((path.to_string(), reason));
            }
            Event::Skipped { path } => self.skipped.push(path.to_string()),
        }
    }

    fn progress(&mut self, entries_seen: u64, bytes_expanded: u64) {
        self.progress.push((entries_seen, bytes_expanded));
    }
}

impl Recorder {
    pub fn paths(&self) -> Vec<&str> {
        let mut v: Vec<&str> = self.leaves.iter().map(|l| l.path.as_str()).collect();
        v.sort();
        v
    }
    pub fn leaf(&self, path: &str) -> &Rec {
        self.leaves
            .iter()
            .find(|l| l.path == path)
            .unwrap_or_else(|| panic!("no leaf {path}: {:?}", self.paths()))
    }
}

pub fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

pub fn sha(b: &[u8]) -> String {
    hex(&Sha256::digest(b))
}

pub struct Env {
    pub _dir: tempfile::TempDir,
    pub tmp: std::path::PathBuf,
    pub input: std::path::PathBuf,
}

pub fn env(bytes: &[u8]) -> Env {
    let dir = tempfile::tempdir().unwrap();
    let tmp = dir.path().join("tmp");
    std::fs::create_dir(&tmp).unwrap();
    let input = dir.path().join("input.bin");
    std::fs::write(&input, bytes).unwrap();
    Env {
        _dir: dir,
        tmp,
        input,
    }
}

pub fn run(
    bytes: &[u8],
    name: &str,
    limits: &Limits,
) -> (Result<WalkStats, WalkError>, Recorder, Env) {
    let (stats, r, rec, e) = run_rec(bytes, name, limits, Recorder::default());
    (r.map(|()| stats), rec, e)
}

/// Like [`run`] with a prepared recorder; returns the stats even on error.
pub fn run_rec(
    bytes: &[u8],
    name: &str,
    limits: &Limits,
    mut rec: Recorder,
) -> (WalkStats, Result<(), WalkError>, Recorder, Env) {
    let e = env(bytes);
    let (stats, r) = walk_file(&e.input, name, &e.tmp, limits, &mut rec);
    (stats, r, rec, e)
}

pub fn assert_tmp_empty(tmp: &Path) {
    let left: Vec<_> = std::fs::read_dir(tmp).unwrap().collect();
    assert!(left.is_empty(), "tmp_dir not empty: {left:?}");
}

// ---------------------------------------------------------------- 7z

/// 7z with one block per file (non-solid). `methods` empty means the
/// writer's default (LZMA2).
pub fn seven_z_with(
    entries: &[(&str, &[u8])],
    methods: Vec<sevenz_rust2::EncoderConfiguration>,
    encrypt_header: bool,
) -> Vec<u8> {
    use sevenz_rust2::{ArchiveEntry, ArchiveWriter};
    let mut w = ArchiveWriter::new(Cursor::new(Vec::new())).unwrap();
    if !methods.is_empty() {
        w.set_content_methods(methods);
    }
    w.set_encrypt_header(encrypt_header);
    for (name, data) in entries {
        if name.ends_with('/') {
            w.push_archive_entry::<&[u8]>(
                ArchiveEntry::new_directory(name.trim_end_matches('/')),
                None,
            )
            .unwrap();
        } else {
            w.push_archive_entry(ArchiveEntry::new_file(name), Some(*data))
                .unwrap();
        }
    }
    w.finish().unwrap().into_inner()
}

pub fn seven_z(entries: &[(&str, &[u8])]) -> Vec<u8> {
    seven_z_with(entries, Vec::new(), false)
}

/// 7z with every file in one solid LZMA2 block.
pub fn seven_z_solid(entries: &[(&str, &[u8])]) -> Vec<u8> {
    use sevenz_rust2::{ArchiveEntry, ArchiveWriter, SourceReader};
    let mut w = ArchiveWriter::new(Cursor::new(Vec::new())).unwrap();
    let names = entries
        .iter()
        .map(|(n, _)| ArchiveEntry::new_file(n))
        .collect();
    let readers = entries.iter().map(|(_, d)| SourceReader::new(*d)).collect();
    w.push_archive_entries(names, readers).unwrap();
    w.finish().unwrap().into_inner()
}

/// 7z with stored (COPY) entries, so content bytes appear verbatim.
pub fn seven_z_stored(entries: &[(&str, &[u8])]) -> Vec<u8> {
    seven_z_with(
        entries,
        vec![sevenz_rust2::EncoderMethod::COPY.into()],
        false,
    )
}

/// 7z with AES-encrypted entries; `encrypt_header` also encrypts the file list.
pub fn seven_z_encrypted(entries: &[(&str, &[u8])], encrypt_header: bool) -> Vec<u8> {
    use sevenz_rust2::encoder_options::{AesEncoderOptions, Lzma2Options};
    seven_z_with(
        entries,
        vec![
            AesEncoderOptions::new("secret".into()).into(),
            Lzma2Options::default().into(),
        ],
        encrypt_header,
    )
}

/// 7z number (7z's own variable-length integer).
pub fn sz_num(v: u64) -> Vec<u8> {
    // n extra bytes (little endian); the first byte has n leading 1 bits
    // and carries the value's high 7-n bits.
    for n in 0..8u32 {
        if v < 1u64 << (8 * n + 7 - n) {
            let mut out = vec![!(0xFFu8 >> n) | (v >> (8 * n)) as u8];
            out.extend((0..n).map(|i| (v >> (8 * i)) as u8));
            return out;
        }
    }
    let mut out = vec![0xFF];
    out.extend_from_slice(&v.to_le_bytes());
    out
}

/// A raw 7z file: signature header, `packed` bytes, then `next_header`
/// (the header database), with correct CRCs.
pub fn seven_z_raw(packed: &[u8], next_header: &[u8]) -> Vec<u8> {
    seven_z_raw_sized(packed, next_header, next_header.len() as u64)
}

/// As `seven_z_raw` but declaring `declared_size` as the header size.
pub fn seven_z_raw_sized(packed: &[u8], next_header: &[u8], declared_size: u64) -> Vec<u8> {
    let mut start = Vec::new();
    start.extend_from_slice(&(packed.len() as u64).to_le_bytes());
    start.extend_from_slice(&declared_size.to_le_bytes());
    start.extend_from_slice(&crc32fast::hash(next_header).to_le_bytes());
    let mut v = b"7z\xBC\xAF\x27\x1C\x00\x04".to_vec();
    v.extend_from_slice(&crc32fast::hash(&start).to_le_bytes());
    v.extend_from_slice(&start);
    v.extend_from_slice(packed);
    v.extend_from_slice(next_header);
    v
}

// ---------------------------------------------------------------- RAR 5

/// Item of a generated RAR 5 archive (all data stored, uncompressed).
pub enum RarItem<'a> {
    File(&'a str, &'a [u8]),
    Dir(&'a str),
    /// A file flagged as AES-encrypted (the data is not really encrypted).
    Encrypted(&'a str, &'a [u8]),
    /// A "file copy" redirection to an earlier entry.
    FileCopy(&'a str, &'a str),
}

/// RAR variable-length integer: 7 bits per byte, low first.
pub fn rar_vint(mut v: u64) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let b = (v & 0x7F) as u8;
        v >>= 7;
        if v == 0 {
            out.push(b);
            return out;
        }
        out.push(b | 0x80);
    }
}

/// One RAR 5 block: CRC32, size, type, flags, [extra size], [data size],
/// `fields`, `extra`, then `data`.
fn rar_block(htype: u64, fields: &[u8], extra: &[u8], data: Option<&[u8]>) -> Vec<u8> {
    let mut flags = 0u64;
    if !extra.is_empty() {
        flags |= 0x01;
    }
    if data.is_some() {
        flags |= 0x02;
    }
    let mut body = rar_vint(htype);
    body.extend(rar_vint(flags));
    if !extra.is_empty() {
        body.extend(rar_vint(extra.len() as u64));
    }
    if let Some(d) = data {
        body.extend(rar_vint(d.len() as u64));
    }
    body.extend_from_slice(fields);
    body.extend_from_slice(extra);
    let mut header = rar_vint(body.len() as u64);
    header.extend(body);
    let mut out = crc32fast::hash(&header).to_le_bytes().to_vec();
    out.extend(header);
    if let Some(d) = data {
        out.extend_from_slice(d);
    }
    out
}

fn rar_extra_record(rtype: u64, data: &[u8]) -> Vec<u8> {
    let mut rec = rar_vint(rtype);
    rec.extend_from_slice(data);
    let mut out = rar_vint(rec.len() as u64);
    out.extend(rec);
    out
}

fn rar_file_fields(name: &str, dir: bool, size: u64, crc: Option<u32>, solid: bool) -> Vec<u8> {
    let mut file_flags = 0u64;
    if dir {
        file_flags |= 0x01;
    }
    if crc.is_some() {
        file_flags |= 0x04;
    }
    let mut f = rar_vint(file_flags);
    f.extend(rar_vint(size));
    f.extend(rar_vint(if dir { 0o40755 } else { 0o100644 }));
    if let Some(c) = crc {
        f.extend_from_slice(&c.to_le_bytes());
    }
    // compression info: version 0, stored (method 0), smallest dictionary
    f.extend(rar_vint(if solid { 0x40 } else { 0 }));
    f.extend(rar_vint(1)); // host OS: Unix
    f.extend(rar_vint(name.len() as u64));
    f.extend_from_slice(name.as_bytes());
    f
}

/// A RAR 5 archive with stored entries. `solid` sets the archive's solid flag.
pub fn rar5(items: &[RarItem<'_>], solid: bool) -> Vec<u8> {
    let mut v = b"Rar!\x1A\x07\x01\x00".to_vec();
    v.extend(rar_block(
        1,
        &rar_vint(if solid { 0x04 } else { 0 }),
        &[],
        None,
    ));
    for (i, it) in items.iter().enumerate() {
        let s = solid && i > 0;
        match it {
            RarItem::File(name, data) => {
                let f = rar_file_fields(
                    name,
                    false,
                    data.len() as u64,
                    Some(crc32fast::hash(data)),
                    s,
                );
                v.extend(rar_block(2, &f, &[], Some(data)));
            }
            RarItem::Dir(name) => {
                let f = rar_file_fields(name, true, 0, None, false);
                v.extend(rar_block(2, &f, &[], None));
            }
            RarItem::Encrypted(name, data) => {
                let mut padded = data.to_vec();
                padded.resize(data.len().div_ceil(16) * 16, 0);
                let mut enc = rar_vint(0); // AES-256
                enc.extend(rar_vint(0)); // no password check, no MAC
                enc.push(15); // KDF count (log2)
                enc.extend_from_slice(&[7u8; 16]); // salt
                enc.extend_from_slice(&[9u8; 16]); // IV
                let extra = rar_extra_record(0x01, &enc);
                let f = rar_file_fields(
                    name,
                    false,
                    data.len() as u64,
                    Some(crc32fast::hash(data)),
                    s,
                );
                v.extend(rar_block(2, &f, &extra, Some(&padded)));
            }
            RarItem::FileCopy(name, target) => {
                let mut r = rar_vint(5); // FSREDIR_FILECOPY
                r.extend(rar_vint(0));
                r.extend(rar_vint(target.len() as u64));
                r.extend_from_slice(target.as_bytes());
                let extra = rar_extra_record(0x05, &r);
                let f = rar_file_fields(name, false, 0, None, s);
                v.extend(rar_block(2, &f, &extra, Some(&[])));
            }
        }
    }
    v.extend(rar_block(5, &rar_vint(0), &[], None));
    v
}

/// A RAR 5 archive whose headers are encrypted: the archive encryption
/// header followed by (fake) encrypted header bytes.
pub fn rar5_encrypted_headers() -> Vec<u8> {
    let mut v = b"Rar!\x1A\x07\x01\x00".to_vec();
    let mut f = rar_vint(0); // AES-256
    f.extend(rar_vint(0)); // no password check
    f.push(15);
    f.extend_from_slice(&[3u8; 16]);
    v.extend(rar_block(4, &f, &[], None));
    v.extend_from_slice(&[5u8; 16]); // IV
    v.extend_from_slice(&[0xAAu8; 64]); // "encrypted" main header
    v
}
