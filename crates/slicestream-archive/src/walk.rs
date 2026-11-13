//! Recursive, streaming walk over an uploaded file: ZIP, tar and gzip
//! containers (nested to `max_depth`), DICOM and NIfTI leaves.
//!
//! Input is untrusted. Entry names are only reported, never used as paths;
//! nested containers are spooled to `NamedTempFile`s in `tmp_dir` that are
//! deleted on drop; nothing is held whole in memory.

use std::fmt;
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

use flate2::read::MultiGzDecoder;
use tempfile::NamedTempFile;

use crate::count::{Budget, Counted};
use crate::{Kind, SNIFF_LEN, detect};

/// Safety limits for one walk (`[archives]` config).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Limits {
    /// Nesting levels allowed below the uploaded file.
    pub max_depth: u32,
    /// File entries seen across the whole upload.
    pub max_entries: u64,
    /// Bytes read out of decoders and entry streams (plus nested spools).
    pub max_expanded_bytes: u64,
    /// Abort when free space in `tmp_dir` falls below this.
    pub min_free_bytes: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            max_depth: 4,
            max_entries: 200_000,
            max_expanded_bytes: 64 * 1024 * 1024 * 1024,
            min_free_bytes: 2 * 1024 * 1024 * 1024,
        }
    }
}

/// A DICOM or NIfTI item found by the walk.
pub struct Leaf<'a> {
    /// `<parent>!/<entry name>`; the top level is the display name.
    pub path: &'a str,
    /// `Kind::Dicom` or `Kind::Nifti`.
    pub kind: Kind,
    /// NIfTI stored gzip-compressed (`.nii.gz`); `reader` yields the gz bytes.
    pub compressed: bool,
    /// First ≤ `SNIFF_LEN` decompressed bytes.
    pub head: &'a [u8],
    /// Raw bytes to store: the gz stream for a compressed NIfTI, else the
    /// plain content. Reads are counted against the limits.
    pub reader: &'a mut dyn Read,
}

#[derive(Debug)]
pub enum Event<'a> {
    /// A container could not be read; its siblings are still walked.
    ContainerFailed { path: &'a str, reason: String },
    /// One entry of a random-access container (ZIP) could not be read
    /// (bad local header, CRC mismatch, unsupported method, encrypted); the
    /// rest of the container is still walked.
    EntryFailed { path: &'a str, reason: String },
    /// An entry that is neither a container, DICOM nor NIfTI (or a DICOMDIR,
    /// or a GNU sparse tar entry).
    Skipped { path: &'a str },
}

pub trait Sink {
    /// Called for DICOM and NIfTI leaves only. An `Err` that is not caused by
    /// the leaf reader stops the walk with `WalkError::Io`.
    ///
    /// If `leaf.reader` returns an error (broken input or a limit abort), the
    /// sink must discard whatever it wrote for this leaf: the bytes are
    /// incomplete. The walker reports the failure itself.
    fn leaf(&mut self, leaf: Leaf<'_>) -> io::Result<()>;
    fn event(&mut self, ev: Event<'_>);
}

#[derive(Debug)]
pub enum WalkError {
    /// A whole-upload limit was crossed; the reason is user-facing.
    Aborted(String),
    /// Local I/O failure (opening the input, spooling, or the sink).
    Io(io::Error),
}

impl fmt::Display for WalkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WalkError::Aborted(r) => f.write_str(r),
            WalkError::Io(e) => write!(f, "i/o error: {e}"),
        }
    }
}

impl std::error::Error for WalkError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            WalkError::Io(e) => Some(e),
            WalkError::Aborted(_) => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WalkStats {
    pub entries_seen: u64,
    pub bytes_expanded: u64,
}

/// Walk `path` (any Kind), reporting leaves and events to `sink`.
pub fn walk_file(
    path: &Path,
    display_name: &str,
    tmp_dir: &Path,
    limits: &Limits,
    sink: &mut dyn Sink,
) -> Result<WalkStats, WalkError> {
    let budget = Budget::new(limits, tmp_dir.to_path_buf());
    let mut w = Walker {
        budget: &budget,
        tmp_dir,
        sink,
    };
    let res = w.top(path, display_name);
    let stats = WalkStats {
        entries_seen: budget.entries(),
        bytes_expanded: budget.bytes(),
    };
    match res {
        Ok(()) => Ok(stats),
        Err(Fail::Abort(r)) => Err(WalkError::Aborted(r)),
        Err(Fail::Io(e)) => Err(WalkError::Io(e)),
        // `container` turns these into events; only reachable if that
        // invariant breaks, so report rather than panic.
        Err(Fail::Container(r)) => Err(WalkError::Io(io::Error::other(r))),
    }
}

/// Internal failure, by blast radius.
enum Fail {
    /// Stop the whole walk (limit crossed).
    Abort(String),
    /// Stop the whole walk (our own I/O or the sink failed).
    Io(io::Error),
    /// The current container is unreadable: report it and continue with its
    /// siblings. The string is the full user-facing reason.
    Container(String),
}

fn cannot_read(e: impl fmt::Display) -> String {
    format!("cannot read archive: {e}")
}

struct Walker<'b> {
    budget: &'b Budget,
    tmp_dir: &'b Path,
    sink: &'b mut dyn Sink,
}

impl Walker<'_> {
    fn top(&mut self, path: &Path, name: &str) -> Result<(), Fail> {
        let mut file = File::open(path).map_err(Fail::Io)?;
        let head = read_head(&mut file).map_err(Fail::Io)?;
        file.seek(SeekFrom::Start(0)).map_err(Fail::Io)?;
        match detect(&head) {
            kind @ (Kind::Dicom | Kind::Nifti | Kind::Other) => {
                self.count_entry()?;
                if kind == Kind::Other || is_dicomdir(&head) {
                    self.sink.event(Event::Skipped { path: name });
                    return Ok(());
                }
                let mut src = Counted::new(file, self.budget, true);
                self.leaf(name, kind, false, &head, &[], &mut src)
            }
            kind => self.container(file, kind, name, 0),
        }
    }

    fn count_entry(&self) -> Result<(), Fail> {
        self.budget.add_entry().map_err(Fail::Abort)
    }

    /// Classify an error from reading untrusted input.
    fn read_fail(&self, e: io::Error) -> Fail {
        match self.budget.aborted() {
            Some(r) => Fail::Abort(r),
            None => Fail::Container(cannot_read(e)),
        }
    }

    /// Classify the outcome of an operation that both reads untrusted input
    /// (`read_error` set by its `Counted`) and writes locally.
    fn settle(&self, res: io::Result<()>, read_error: Option<String>) -> Result<(), Fail> {
        if let Some(r) = self.budget.aborted() {
            return Err(Fail::Abort(r));
        }
        if let Some(m) = read_error {
            return Err(Fail::Container(cannot_read(m)));
        }
        res.map_err(Fail::Io)
    }

    /// Hand a leaf to the sink. `prefix` (already-read head bytes) is
    /// replayed before the rest of `src`.
    fn leaf<R: Read>(
        &mut self,
        path: &str,
        kind: Kind,
        compressed: bool,
        head: &[u8],
        prefix: &[u8],
        src: &mut Counted<'_, R>,
    ) -> Result<(), Fail> {
        let res = {
            let mut reader = prefix.chain(&mut *src);
            self.sink.leaf(Leaf {
                path,
                kind,
                compressed,
                head,
                reader: &mut reader,
            })
        };
        let read_error = src.take_error();
        self.settle(res, read_error)
    }

    /// Walk a container held in a local file. Failures of this container
    /// become a `ContainerFailed` event; aborts and I/O errors propagate.
    fn container(&mut self, file: File, kind: Kind, path: &str, depth: u32) -> Result<(), Fail> {
        let res = match kind {
            Kind::Zip => self.zip(file, path, depth),
            Kind::Tar => self.tar(BufReader::new(file), path, depth, true),
            Kind::Gzip => self.gzip(file, path, depth),
            // Task 4 adds these.
            Kind::SevenZ | Kind::Rar => Err(Fail::Container("unsupported archive type".into())),
            Kind::Dicom | Kind::Nifti | Kind::Other => Ok(()),
        };
        match res {
            Err(Fail::Container(reason)) => {
                self.sink.event(Event::ContainerFailed { path, reason });
                Ok(())
            }
            other => other,
        }
    }

    /// One file entry of a container: classify from its first bytes, then
    /// skip it, hand it to the sink, or spool and walk it as a nested container.
    fn entry<R: Read>(
        &mut self,
        src: &mut Counted<'_, R>,
        path: &str,
        depth: u32,
    ) -> Result<(), Fail> {
        self.count_entry()?;
        let head = read_head(src).map_err(|e| self.read_fail(e))?;
        match detect(&head) {
            Kind::Other => {
                self.sink.event(Event::Skipped { path });
                Ok(())
            }
            Kind::Dicom if is_dicomdir(&head) => {
                self.sink.event(Event::Skipped { path });
                Ok(())
            }
            kind @ (Kind::Dicom | Kind::Nifti) => self.leaf(path, kind, false, &head, &head, src),
            kind => self.nested(kind, &head, src, path, depth),
        }
    }

    /// A container read from a stream (`head` already read from `src`):
    /// check depth, spool to a temp file in `tmp_dir`, walk it at depth+1.
    fn nested<R: Read>(
        &mut self,
        kind: Kind,
        head: &[u8],
        src: &mut Counted<'_, R>,
        path: &str,
        depth: u32,
    ) -> Result<(), Fail> {
        let max = self.budget.limits().max_depth;
        let depth = depth.saturating_add(1);
        if depth > max {
            self.sink.event(Event::ContainerFailed {
                path,
                reason: format!("too deeply nested (limit {max})"),
            });
            return Ok(());
        }
        let spool = self.spool(head, src)?;
        let mut file = spool.as_file().try_clone().map_err(Fail::Io)?;
        file.seek(SeekFrom::Start(0)).map_err(Fail::Io)?;
        self.container(file, kind, path, depth)
        // `spool` is dropped (and deleted) here.
    }

    /// Copy `head` + the rest of `src` to a temp file in `tmp_dir`. The bytes
    /// are counted by `src` as they are read.
    fn spool<R: Read>(&self, head: &[u8], src: &mut Counted<'_, R>) -> Result<NamedTempFile, Fail> {
        let tmp = NamedTempFile::new_in(self.tmp_dir).map_err(Fail::Io)?;
        let res = (|| {
            let mut w = BufWriter::with_capacity(64 * 1024, tmp.as_file());
            w.write_all(head)?;
            io::copy(src, &mut w)?;
            w.flush()
        })();
        let read_error = src.take_error();
        self.settle(res, read_error)?;
        Ok(tmp)
    }

    fn zip(&mut self, file: File, path: &str, depth: u32) -> Result<(), Fail> {
        // The zip crate keeps metadata for every central-directory record in
        // memory: refuse an archive whose directory is too big, or that
        // declares more entries than the budget has left, before it parses.
        let cd = zip_central_directory(&file).map_err(Fail::Io)?;
        if cd.bytes > MAX_ZIP_CENTRAL_DIRECTORY {
            return Err(Fail::Container(cannot_read("central directory too large")));
        }
        let limits = self.budget.limits();
        if cd.entries > limits.max_entries.saturating_sub(self.budget.entries()) {
            return Err(Fail::Abort(self.budget.abort_entries()));
        }
        let mut ar = zip::ZipArchive::new(BufReader::new(file))
            .map_err(|e| Fail::Container(cannot_read(e)))?;
        // Every file encrypted: the archive is. Some encrypted: those entries.
        let mut files = 0usize;
        let mut encrypted = 0usize;
        for i in 0..ar.len() {
            let meta = ar
                .by_index_data(i)
                .map_err(|e| Fail::Container(cannot_read(e)))?;
            if !(meta.is_dir() || meta.is_symlink()) {
                files += 1;
                encrypted += usize::from(meta.encrypted());
            }
        }
        if files > 0 && encrypted == files {
            return Err(Fail::Container("encrypted archive".into()));
        }
        for i in 0..ar.len() {
            let (name, is_encrypted) = {
                let meta = ar
                    .by_index_data(i)
                    .map_err(|e| Fail::Container(cannot_read(e)))?;
                if meta.is_dir() || meta.is_symlink() {
                    continue;
                }
                let name = meta
                    .name()
                    .map(|n| n.into_owned())
                    .unwrap_or_else(|_| String::from_utf8_lossy(meta.name_raw()).into_owned());
                (name, meta.encrypted())
            };
            let entry_path = format!("{path}!/{name}");
            let res = if is_encrypted {
                self.count_entry()?;
                Err(Fail::Container("encrypted archive".into()))
            } else {
                match ar.by_index(i) {
                    Ok(zf) => {
                        let mut src = Counted::new(zf, self.budget, true);
                        self.entry(&mut src, &entry_path, depth)
                    }
                    Err(e) => {
                        self.count_entry()?;
                        Err(Fail::Container(cannot_read(e)))
                    }
                }
            };
            // Random access: one bad entry fails only itself.
            match res {
                Err(Fail::Container(reason)) => self.sink.event(Event::EntryFailed {
                    path: &entry_path,
                    reason,
                }),
                other => other?,
            }
        }
        Ok(())
    }

    /// Walk a tar stream. `count_entries`: charge entry reads to the budget
    /// (false when `r` is already a counted decoder, to avoid double counting).
    ///
    /// Iterates raw headers so that GNU long-name/long-link and pax headers,
    /// which the tar crate would read whole into memory, are size-capped here.
    fn tar<R: Read>(
        &mut self,
        r: R,
        path: &str,
        depth: u32,
        count_entries: bool,
    ) -> Result<(), Fail> {
        let mut ar = tar::Archive::new(r);
        let entries = ar.entries().map_err(|e| self.read_fail(e))?.raw(true);
        let mut long_name: Option<Vec<u8>> = None;
        let mut pax_path: Option<Vec<u8>> = None;
        let mut pax_size: Option<u64> = None;
        for item in entries {
            let mut entry = item.map_err(|e| self.read_fail(e))?;
            let et = entry.header().entry_type();
            // Raw size as stored (the tar crate skips data by this); `size()`
            // would report a sparse file's expanded size instead.
            let size = entry.header().entry_size().map_err(|e| self.read_fail(e))?;
            if et.is_gnu_longname()
                || et.is_gnu_longlink()
                || et.is_pax_local_extensions()
                || et.is_pax_global_extensions()
            {
                if size > MAX_TAR_META {
                    return Err(Fail::Container(cannot_read(
                        "oversized tar metadata header",
                    )));
                }
                let mut data = Vec::new();
                (&mut entry)
                    .take(size)
                    .read_to_end(&mut data)
                    .map_err(|e| self.read_fail(e))?;
                if et.is_gnu_longname() {
                    while data.last() == Some(&0) {
                        data.pop();
                    }
                    long_name = Some(data);
                } else if et.is_pax_local_extensions() {
                    for ext in tar::PaxExtensions::new(&data) {
                        let ext = ext.map_err(|e| self.read_fail(e))?;
                        match ext.key_bytes() {
                            b"path" => pax_path = Some(ext.value_bytes().to_vec()),
                            b"size" => {
                                let v = ext
                                    .value()
                                    .ok()
                                    .and_then(|v| v.parse::<u64>().ok())
                                    .ok_or_else(|| Fail::Container(cannot_read("bad pax size")))?;
                                pax_size = Some(v);
                            }
                            _ => {}
                        }
                    }
                }
                continue;
            }
            // Raw iteration skips entry data by the header size; a pax size
            // override would desynchronise the stream, so refuse it.
            if let Some(ps) = pax_size.take()
                && ps != size
            {
                return Err(Fail::Container(cannot_read(
                    "unsupported pax size override",
                )));
            }
            let pax = pax_path.take();
            let long = long_name.take();
            let name_bytes = match pax.or(long) {
                Some(n) => n,
                None => entry.path_bytes().into_owned(),
            };
            let entry_path = format!("{path}!/{}", String::from_utf8_lossy(&name_bytes));
            if et.is_gnu_sparse() {
                // Raw iteration does not consume extended sparse header
                // blocks; skipping by entry size would desynchronise.
                if entry.header().as_gnu().is_some_and(|g| g.is_extended()) {
                    return Err(Fail::Container(cannot_read(
                        "unsupported extended sparse tar entry",
                    )));
                }
                self.count_entry()?;
                self.sink.event(Event::Skipped { path: &entry_path });
                continue;
            }
            // Only plain files; links, devices and directories are ignored.
            if !(et.is_file() || et.is_contiguous()) {
                continue;
            }
            let mut src = Counted::new(entry, self.budget, count_entries);
            self.entry(&mut src, &entry_path, depth)?;
        }
        Ok(())
    }

    /// gzip: peek the decompressed head and classify the inner stream. The
    /// decompressed layer keeps the gzip's path: `x.zip.gz!/IM1`.
    fn gzip(&mut self, file: File, path: &str, depth: u32) -> Result<(), Fail> {
        let budget = self.budget;
        let mut dec = Counted::new(MultiGzDecoder::new(BufReader::new(&file)), budget, true);
        let head = read_head(&mut dec).map_err(|e| self.read_fail(e))?;
        match detect(&head) {
            Kind::Tar => {
                let r = (&head[..]).chain(&mut dec);
                self.tar(r, path, depth, false)
            }
            Kind::Nifti => {
                drop(dec);
                let mut raw = &file;
                raw.seek(SeekFrom::Start(0)).map_err(Fail::Io)?;
                let mut src = Counted::new(raw, budget, true);
                self.leaf(path, Kind::Nifti, true, &head, &[], &mut src)
            }
            Kind::Dicom if !is_dicomdir(&head) => {
                self.leaf(path, Kind::Dicom, false, &head, &head, &mut dec)
            }
            kind @ (Kind::Zip | Kind::SevenZ | Kind::Rar | Kind::Gzip) => {
                self.nested(kind, &head, &mut dec, path, depth)
            }
            Kind::Dicom | Kind::Other => {
                self.sink.event(Event::Skipped { path });
                Ok(())
            }
        }
    }
}

/// Cap on a ZIP central directory (the zip crate holds it all in memory).
const MAX_ZIP_CENTRAL_DIRECTORY: u64 = 256 * 1024 * 1024;
/// Cap on a tar GNU long-name/long-link or pax header's data.
const MAX_TAR_META: u64 = 1024 * 1024;

#[derive(Debug, Default, PartialEq, Eq)]
struct CentralDirectory {
    /// Largest declared entry count.
    entries: u64,
    /// Largest of the declared directory sizes and the real span from the
    /// declared directory offset to its end record.
    bytes: u64,
}

/// Look at every end-of-central-directory record (ZIP64 included) in the
/// last 64 KiB + 22 bytes, so a decoy record cannot hide a bigger one, and
/// report the worst case. All zeros if there is none.
fn zip_central_directory(mut file: &File) -> io::Result<CentralDirectory> {
    const EOCD: &[u8] = b"PK\x05\x06";
    const LOC64: &[u8] = b"PK\x06\x07";
    const EOCD64: &[u8] = b"PK\x06\x06";
    let len = file.seek(SeekFrom::End(0))?;
    let start = len.saturating_sub(22 + 65_535);
    file.seek(SeekFrom::Start(start))?;
    let mut tail = Vec::new();
    file.take(len - start).read_to_end(&mut tail)?;
    let le = |b: &[u8], o: usize, n: usize| -> u64 {
        b.get(o..o + n)
            .map(|x| x.iter().rev().fold(0u64, |a, &v| (a << 8) | u64::from(v)))
            .unwrap_or(0)
    };
    let mut cd = CentralDirectory::default();
    for i in 0..tail.len().saturating_sub(21) {
        if tail.get(i..i + 4) != Some(EOCD) {
            continue;
        }
        let pos = start + i as u64;
        let entries32 = le(&tail, i + 10, 2);
        let size32 = le(&tail, i + 12, 4);
        let offset32 = le(&tail, i + 16, 4);
        // ZIP64: a locator right before the EOCD points at the ZIP64 record,
        // whose values replace 32-bit fields holding the 0xFFFF.. sentinel.
        let mut zip64 = None;
        if let Some(loc_pos) = pos.checked_sub(20) {
            let mut loc = [0u8; 20];
            file.seek(SeekFrom::Start(loc_pos))?;
            file.read_exact(&mut loc)?;
            let off = le(&loc, 8, 8);
            if loc.get(0..4) == Some(LOC64) && off.saturating_add(56) <= len {
                let mut rec = [0u8; 56];
                file.seek(SeekFrom::Start(off))?;
                file.read_exact(&mut rec)?;
                if rec.get(0..4) == Some(EOCD64) {
                    zip64 = Some((off, rec));
                }
            }
        }
        let real = |v: u64, sentinel: u64| !(zip64.is_some() && v == sentinel);
        if real(entries32, 0xFFFF) {
            cd.entries = cd.entries.max(entries32);
        }
        if real(size32, 0xFFFF_FFFF) {
            cd.bytes = cd.bytes.max(size32);
        }
        if real(offset32, 0xFFFF_FFFF) {
            cd.bytes = cd.bytes.max(pos.saturating_sub(offset32));
        }
        if let Some((off, rec)) = zip64 {
            cd.entries = cd.entries.max(le(&rec, 32, 8));
            let size = le(&rec, 40, 8);
            let offset = le(&rec, 48, 8);
            cd.bytes = cd.bytes.max(size).max(off.saturating_sub(offset));
        }
    }
    file.seek(SeekFrom::Start(0))?;
    Ok(cd)
}

/// Read up to `SNIFF_LEN` bytes (fewer only at end of stream).
fn read_head<R: Read + ?Sized>(r: &mut R) -> io::Result<Vec<u8>> {
    let mut buf = vec![0u8; SNIFF_LEN];
    let mut n = 0;
    while n < buf.len() {
        match r.read(&mut buf[n..]) {
            Ok(0) => break,
            Ok(k) => n += k,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    buf.truncate(n);
    Ok(buf)
}

/// A DICOMDIR: Part-10 whose Media Storage SOP Class is
/// 1.2.840.10008.1.3.10 (Media Storage Directory Storage).
fn is_dicomdir(head: &[u8]) -> bool {
    const UID: &[u8] = b"1.2.840.10008.1.3.10";
    let meta = head.get(132..).unwrap_or_default();
    meta.windows(UID.len()).enumerate().any(|(i, w)| {
        w == UID && !matches!(meta.get(i + UID.len()), Some(c) if c.is_ascii_digit() || *c == b'.')
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn small_zip() -> Vec<u8> {
        use std::io::Write;
        let mut w = zip::ZipWriter::new(io::Cursor::new(Vec::new()));
        w.start_file("a", zip::write::SimpleFileOptions::default())
            .unwrap();
        w.write_all(b"hello").unwrap();
        w.finish().unwrap().into_inner()
    }

    fn cd_of(bytes: &[u8]) -> CentralDirectory {
        let mut f = tempfile::tempfile().unwrap();
        f.write_all(bytes).unwrap();
        zip_central_directory(&f).unwrap()
    }

    #[test]
    fn central_directory_bounds() {
        let z = small_zip();
        let eocd = z.windows(4).rposition(|w| w == b"PK\x05\x06").unwrap();
        let declared = u32::from_le_bytes(z[eocd + 12..eocd + 16].try_into().unwrap());
        let cd = cd_of(&z);
        assert_eq!(cd.entries, 1);
        assert_eq!(cd.bytes, u64::from(declared));

        // under-declared size: the real span from the offset still counts
        let mut z2 = z.clone();
        z2[eocd + 12..eocd + 16].copy_from_slice(&0u32.to_le_bytes());
        z2[eocd + 16..eocd + 20].copy_from_slice(&0u32.to_le_bytes());
        assert_eq!(cd_of(&z2).bytes, eocd as u64);

        // a decoy record at the end cannot hide a bigger one before it
        let mut z3 = z.clone();
        let mut decoy = z[eocd..eocd + 22].to_vec();
        decoy[10..12].copy_from_slice(&0u16.to_le_bytes());
        decoy[12..16].copy_from_slice(&0u32.to_le_bytes());
        z3.extend_from_slice(&decoy);
        assert_eq!(cd_of(&z3).entries, 1);

        assert_eq!(cd_of(b"not a zip"), CentralDirectory::default());
    }

    #[test]
    fn dicomdir_uid_must_end() {
        let mut h = vec![0u8; 132];
        h.extend_from_slice(b"xx1.2.840.10008.1.3.10\0");
        assert!(is_dicomdir(&h));
        let mut h = vec![0u8; 132];
        h.extend_from_slice(b"1.2.840.10008.1.3.101");
        assert!(!is_dicomdir(&h));
        assert!(!is_dicomdir(b"short"));
    }
}
