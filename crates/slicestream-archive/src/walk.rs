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
    /// An entry that is neither a container, DICOM nor NIfTI (or a DICOMDIR).
    Skipped { path: &'a str },
}

pub trait Sink {
    /// Called for DICOM and NIfTI leaves only. An `Err` that is not caused by
    /// the leaf reader stops the walk with `WalkError::Io`.
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
            kind => {
                let max = self.budget.limits().max_depth;
                let depth = depth.saturating_add(1);
                if depth > max {
                    self.sink.event(Event::ContainerFailed {
                        path,
                        reason: format!("too deeply nested (limit {max})"),
                    });
                    return Ok(());
                }
                let spool = self.spool(&head, src)?;
                let mut file = spool.as_file().try_clone().map_err(Fail::Io)?;
                file.seek(SeekFrom::Start(0)).map_err(Fail::Io)?;
                self.container(file, kind, path, depth)
                // `spool` is dropped (and deleted) here.
            }
        }
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
        // The zip crate keeps metadata for every central-directory entry in
        // memory, so refuse an archive that declares more entries than the
        // budget has left before letting it parse them.
        let declared = zip_declared_entries(&file).map_err(Fail::Io)?;
        let limits = self.budget.limits();
        if declared > limits.max_entries.saturating_sub(self.budget.entries()) {
            return Err(Fail::Abort(self.budget.abort_entries()));
        }
        let mut ar = zip::ZipArchive::new(BufReader::new(file))
            .map_err(|e| Fail::Container(cannot_read(e)))?;
        // Refuse an archive with any encrypted entry before emitting anything.
        for i in 0..ar.len() {
            let meta = ar
                .by_index_data(i)
                .map_err(|e| Fail::Container(cannot_read(e)))?;
            if meta.encrypted() {
                return Err(Fail::Container("encrypted archive".into()));
            }
        }
        for i in 0..ar.len() {
            let name = {
                let meta = ar
                    .by_index_data(i)
                    .map_err(|e| Fail::Container(cannot_read(e)))?;
                if meta.is_dir() || meta.is_symlink() {
                    continue;
                }
                meta.name()
                    .map_err(|e| Fail::Container(cannot_read(e)))?
                    .into_owned()
            };
            let entry_path = format!("{path}!/{name}");
            let zf = ar
                .by_index(i)
                .map_err(|e| Fail::Container(cannot_read(e)))?;
            let mut src = Counted::new(zf, self.budget, true);
            self.entry(&mut src, &entry_path, depth)?;
        }
        Ok(())
    }

    /// Walk a tar stream. `count_entries`: charge entry reads to the budget
    /// (false when `r` is already a counted decoder, to avoid double counting).
    fn tar<R: Read>(
        &mut self,
        r: R,
        path: &str,
        depth: u32,
        count_entries: bool,
    ) -> Result<(), Fail> {
        let mut ar = tar::Archive::new(r);
        let entries = ar.entries().map_err(|e| self.read_fail(e))?;
        for item in entries {
            let entry = item.map_err(|e| self.read_fail(e))?;
            let et = entry.header().entry_type();
            // Only plain files. Links, devices and directories are ignored;
            // GNU sparse entries too, since their expansion would not pass
            // through a counted decoder.
            if !(et.is_file() || et.is_contiguous()) {
                continue;
            }
            let name = String::from_utf8_lossy(&entry.path_bytes()).into_owned();
            let entry_path = format!("{path}!/{name}");
            let mut src = Counted::new(entry, self.budget, count_entries);
            self.entry(&mut src, &entry_path, depth)?;
        }
        Ok(())
    }

    /// gzip: peek the decompressed head and classify the inner stream.
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
            _ => {
                self.sink.event(Event::Skipped { path });
                Ok(())
            }
        }
    }
}

/// The largest entry count declared by any end-of-central-directory record
/// (ZIP64 included) in the last 64 KiB + 22 bytes of the file; 0 if none.
/// Every candidate is considered, so a decoy record cannot hide a bigger one.
fn zip_declared_entries(mut file: &File) -> io::Result<u64> {
    const EOCD: &[u8] = b"PK\x05\x06";
    const LOC64: &[u8] = b"PK\x06\x07";
    const EOCD64: &[u8] = b"PK\x06\x06";
    let len = file.seek(SeekFrom::End(0))?;
    let start = len.saturating_sub(22 + 65_535);
    file.seek(SeekFrom::Start(start))?;
    let mut tail = Vec::new();
    file.take(len - start).read_to_end(&mut tail)?;
    let u16_at = |b: &[u8], o: usize| b.get(o..o + 2).map(|x| u16::from_le_bytes([x[0], x[1]]));
    let u64_at = |b: &[u8], o: usize| {
        b.get(o..o + 8)
            .and_then(|x| x.try_into().ok())
            .map(u64::from_le_bytes)
    };
    let mut max = 0u64;
    for i in 0..tail.len().saturating_sub(21) {
        if tail.get(i..i + 4) != Some(EOCD) {
            continue;
        }
        let n = u16_at(&tail, i + 10).unwrap_or(0);
        max = max.max(u64::from(n));
        // ZIP64 locator sits right before the EOCD and points at the record.
        let Some(loc) = i.checked_sub(20) else {
            continue;
        };
        if tail.get(loc..loc + 4) != Some(LOC64) {
            continue;
        }
        let Some(off) = u64_at(&tail, loc + 8) else {
            continue;
        };
        if off.saturating_add(56) > len {
            continue;
        }
        let mut rec = [0u8; 56];
        file.seek(SeekFrom::Start(off))?;
        file.read_exact(&mut rec)?;
        if rec.get(0..4) == Some(EOCD64) {
            max = max.max(u64_at(&rec, 32).unwrap_or(0));
        }
    }
    file.seek(SeekFrom::Start(0))?;
    Ok(max)
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
