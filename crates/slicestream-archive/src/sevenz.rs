//! Pre-check of a 7z header database before `sevenz-rust2` parses it.
//!
//! `sevenz-rust2` trusts the header: it allocates whatever counts and sizes
//! it declares, indexes with them, and follows coder bind pairs without a
//! cycle check. This module walks the same grammar first, with the same
//! decisions, and refuses anything that would make the library allocate
//! without bound, index out of range, overflow, or loop. It also refuses
//! dictionaries over `MAX_DICT` (decoders allocate and zero the whole
//! dictionary) and reports encrypted headers.

use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};

use crate::count::Budget;

/// Cap on the header database (raw and, if compressed, decoded).
pub(crate) const MAX_HEADER: u64 = 256 * 1024 * 1024;
/// Cap on an LZMA/LZMA2 dictionary.
pub(crate) const MAX_DICT: u64 = 256 * 1024 * 1024;
/// Coders per folder (`sevenz-rust2`'s own limit for complex folders).
const MAX_CODERS: u64 = 32;
/// Input streams per coder (BCJ2 has 4).
const MAX_CODER_INPUTS: u64 = 32;
const SIGNATURE_HEADER: u64 = 32;

pub(crate) const ID_COPY: &[u8] = &[0x00];
pub(crate) const ID_LZMA: &[u8] = &[0x03, 0x01, 0x01];
pub(crate) const ID_LZMA2: &[u8] = &[0x21];
pub(crate) const ID_AES: &[u8] = &[0x06, 0xF1, 0x07, 0x01];

#[derive(Debug)]
pub(crate) enum Problem {
    /// Declares more entries than the budget has left.
    TooManyEntries,
    /// The header database is encrypted.
    Encrypted,
    /// Malformed or outside our limits; the string is the detail.
    Bad(String),
    /// A limit was crossed while decoding the header.
    Abort(String),
    /// Local I/O error.
    Io(io::Error),
}

fn bad<T>(m: &str) -> Result<T, Problem> {
    Err(Problem::Bad(m.to_string()))
}

/// Outcome of a successful check.
pub(crate) struct Checked {
    /// The archive has no header database (an empty archive).
    pub(crate) empty: bool,
}

/// Check the archive in `file`. `remaining` is how many entries the budget
/// has left; decoded header bytes are charged to `budget`.
pub(crate) fn check(file: &File, remaining: u64, budget: &Budget) -> Result<Checked, Problem> {
    let mut f = file;
    let len = f.seek(SeekFrom::End(0)).map_err(Problem::Io)?;
    f.seek(SeekFrom::Start(0)).map_err(Problem::Io)?;
    if len < SIGNATURE_HEADER {
        return bad("truncated");
    }
    let mut sh = [0u8; 32];
    f.read_exact(&mut sh).map_err(Problem::Io)?;
    if sh[6] != 0 {
        return bad("unsupported version");
    }
    let start_crc = le(&sh[8..12]);
    let start = &sh[12..32];
    if start_crc == 0 && start.iter().all(|&b| b == 0) {
        // sevenz-rust2 would scan for a header; an unfinished archive.
        return bad("missing start header");
    }
    if u64::from(crc32fast::hash(start)) != start_crc {
        return bad("start header checksum mismatch");
    }
    let offset = le(&start[0..8]);
    let size = le(&start[8..16]);
    let crc = le(&start[16..20]);
    if size == 0 {
        return Ok(Checked { empty: true });
    }
    if size > MAX_HEADER {
        return bad("header too large");
    }
    let pos = SIGNATURE_HEADER
        .checked_add(offset)
        .filter(|p| p.checked_add(size).is_some_and(|end| end <= len))
        .ok_or_else(|| Problem::Bad("truncated".into()))?;
    let mut raw = vec![0u8; size as usize];
    f.seek(SeekFrom::Start(pos)).map_err(Problem::Io)?;
    f.read_exact(&mut raw).map_err(Problem::Io)?;
    if u64::from(crc32fast::hash(&raw)) != crc {
        return bad("header checksum mismatch");
    }
    let caps = Caps { remaining, len };
    let mut p = P::new(&raw);
    match p.u8()? {
        0x01 => header(&mut p, caps)?,
        0x17 => {
            let decoded = encoded_header(&mut p, f, caps, budget)?;
            drop(raw);
            let mut p = P::new(&decoded);
            if p.u8()? != 0x01 {
                return bad("no header");
            }
            header(&mut p, caps)?;
        }
        _ => return bad("no header"),
    }
    Ok(Checked { empty: false })
}

fn le(b: &[u8]) -> u64 {
    b.iter().rev().fold(0u64, |a, &v| (a << 8) | u64::from(v))
}

#[derive(Clone, Copy)]
struct Caps {
    /// Entries the budget has left.
    remaining: u64,
    /// Archive file length.
    len: u64,
}

impl Caps {
    /// A declared per-file count: more than the budget has left aborts.
    fn files(&self, n: u64) -> Result<usize, Problem> {
        if n > self.remaining {
            return Err(Problem::TooManyEntries);
        }
        usize::try_from(n).map_err(|_| Problem::TooManyEntries)
    }
}

/// Cursor over header bytes.
struct P<'a> {
    b: &'a [u8],
    pos: usize,
}

impl<'a> P<'a> {
    fn new(b: &'a [u8]) -> Self {
        P { b, pos: 0 }
    }
    fn left(&self) -> u64 {
        (self.b.len() - self.pos) as u64
    }
    fn bytes(&mut self, n: u64) -> Result<&'a [u8], Problem> {
        if n > self.left() {
            return bad("truncated header");
        }
        let s = &self.b[self.pos..self.pos + n as usize];
        self.pos += n as usize;
        Ok(s)
    }
    fn u8(&mut self) -> Result<u8, Problem> {
        Ok(self.bytes(1)?[0])
    }
    /// 7z number (as `sevenz-rust2` reads it).
    fn num(&mut self) -> Result<u64, Problem> {
        let first = u64::from(self.u8()?);
        let mut mask = 0x80u64;
        let mut value = 0u64;
        for i in 0..8 {
            if first & mask == 0 {
                return Ok(value | ((first & (mask - 1)) << (8 * i)));
            }
            value |= u64::from(self.u8()?) << (8 * i);
            mask >>= 1;
        }
        Ok(value)
    }
    /// `n` bits, high bit first; returns how many are set.
    fn bits(&mut self, n: usize) -> Result<Vec<bool>, Problem> {
        let bytes = self.bytes(n.div_ceil(8) as u64)?;
        Ok((0..n)
            .map(|i| bytes[i / 8] & (0x80 >> (i % 8)) != 0)
            .collect())
    }
    fn all_or_bits(&mut self, n: usize) -> Result<Vec<bool>, Problem> {
        if self.u8()? != 0 {
            Ok(vec![true; n])
        } else {
            self.bits(n)
        }
    }
}

fn popcount(v: &[bool]) -> usize {
    v.iter().filter(|&&b| b).count()
}

struct Coder {
    id: Vec<u8>,
    props: Vec<u8>,
}

struct Folder {
    coders: Vec<Coder>,
    packed: usize,
    unpack_sizes: Vec<u64>,
    has_crc: bool,
    crc: u64,
    sub_streams: usize,
}

impl Folder {
    /// Final output size: the one output not bound to an input.
    fn unpack_size(&self, bound_outs: &[u64]) -> u64 {
        (0..self.unpack_sizes.len())
            .rev()
            .find(|i| !bound_outs.contains(&(*i as u64)))
            .map(|i| self.unpack_sizes[i])
            .unwrap_or(0)
    }
}

#[derive(Default)]
struct Streams {
    pack_pos: u64,
    pack_sizes: Option<Vec<u64>>,
    folders: Vec<(Folder, Vec<u64>)>,
    /// Total unpack streams, if a SubStreamsInfo was present.
    sub_total: Option<usize>,
}

fn streams(p: &mut P<'_>, caps: Caps) -> Result<Streams, Problem> {
    let mut s = Streams::default();
    let mut nid = p.u8()?;
    if nid == 0x06 {
        pack_info(p, caps, &mut s)?;
        nid = p.u8()?;
    }
    if nid == 0x07 {
        unpack_info(p, caps, &mut s)?;
        nid = p.u8()?;
    }
    if nid == 0x08 {
        sub_streams(p, caps, &mut s)?;
        nid = p.u8()?;
    }
    if nid != 0x00 {
        return bad("bad streams info");
    }
    // Pack streams must cover the folders and lie inside the file.
    let needed: usize = s.folders.iter().map(|(f, _)| f.packed).sum();
    let sizes = s.pack_sizes.as_deref().unwrap_or_default();
    if needed > sizes.len() {
        return bad("missing pack streams");
    }
    let mut end = SIGNATURE_HEADER.checked_add(s.pack_pos);
    for &sz in sizes {
        end = end.and_then(|e| e.checked_add(sz));
    }
    if end.is_none_or(|e| e > caps.len) {
        return bad("pack streams beyond end of file");
    }
    Ok(s)
}

fn pack_info(p: &mut P<'_>, caps: Caps, s: &mut Streams) -> Result<(), Problem> {
    s.pack_pos = p.num()?;
    let n = p.num()?;
    // Each pack stream has a size (at least one byte) and is at most
    // MAX_CODER_INPUTS per folder.
    if n > caps
        .remaining
        .saturating_add(1)
        .saturating_mul(MAX_CODER_INPUTS)
    {
        return Err(Problem::TooManyEntries);
    }
    if n > p.left() {
        return bad("truncated header");
    }
    let n = n as usize;
    let mut nid = p.u8()?;
    if nid == 0x09 {
        let mut sizes = Vec::with_capacity(n);
        for _ in 0..n {
            sizes.push(p.num()?);
        }
        s.pack_sizes = Some(sizes);
        nid = p.u8()?;
    }
    if nid == 0x0A {
        let defined = p.all_or_bits(n)?;
        p.bytes(4 * popcount(&defined) as u64)?;
        nid = p.u8()?;
    }
    if nid != 0x00 {
        return bad("bad pack info");
    }
    Ok(())
}

fn unpack_info(p: &mut P<'_>, caps: Caps, s: &mut Streams) -> Result<(), Problem> {
    if p.u8()? != 0x0B {
        return bad("bad unpack info");
    }
    let n = p.num()?;
    // Every folder holds at least one file in a real archive (+1: the
    // folder of an encoded header).
    let n = caps.files(n.saturating_sub(1))?.saturating_add(1);
    if n as u64 > p.left() {
        return bad("truncated header");
    }
    if p.u8()? != 0 {
        return bad("external folders");
    }
    let mut folders = Vec::with_capacity(n);
    for _ in 0..n {
        folders.push(folder(p)?);
    }
    if p.u8()? != 0x0C {
        return bad("bad unpack info");
    }
    for (f, _) in folders.iter_mut() {
        for _ in 0..f.coders.len() {
            f.unpack_sizes.push(p.num()?);
        }
    }
    let mut nid = p.u8()?;
    if nid == 0x0A {
        let defined = p.all_or_bits(n)?;
        for (i, (f, _)) in folders.iter_mut().enumerate() {
            if defined[i] {
                f.has_crc = true;
                f.crc = le(p.bytes(4)?);
            }
        }
        nid = p.u8()?;
    }
    if nid != 0x00 {
        return bad("bad unpack info");
    }
    for (f, _) in &folders {
        check_dictionaries(f)?;
    }
    s.folders = folders;
    Ok(())
}

/// One folder; returns it with its bound output indices.
fn folder(p: &mut P<'_>) -> Result<(Folder, Vec<u64>), Problem> {
    let nc = p.num()?;
    if nc == 0 || nc > MAX_CODERS {
        return bad("unsupported coder count");
    }
    let mut coders = Vec::new();
    // owner[i] = coder that owns input stream i
    let mut owner = Vec::new();
    for c in 0..nc {
        let flags = p.u8()?;
        let id = p.bytes(u64::from(flags & 0x0F))?.to_vec();
        let inputs = if flags & 0x10 == 0 {
            1
        } else {
            let i = p.num()?;
            if p.num()? != 1 {
                return bad("unsupported coder outputs");
            }
            i
        };
        if inputs == 0 || inputs > MAX_CODER_INPUTS {
            return bad("unsupported coder inputs");
        }
        let props = if flags & 0x20 != 0 {
            let n = p.num()?;
            p.bytes(n)?.to_vec()
        } else {
            Vec::new()
        };
        if flags & 0x80 != 0 {
            return bad("alternative coder methods");
        }
        owner.extend(std::iter::repeat_n(c, inputs as usize));
        coders.push(Coder { id, props });
    }
    let total_in = owner.len() as u64;
    let total_out = nc;
    let pairs = total_out - 1;
    if total_in < pairs {
        return bad("bad bind pairs");
    }
    let mut bound_in = Vec::new();
    let mut bound_out = Vec::new();
    for _ in 0..pairs {
        let i = p.num()?;
        let o = p.num()?;
        if i >= total_in || o >= total_out || bound_in.contains(&i) || bound_out.contains(&o) {
            return bad("bad bind pairs");
        }
        bound_in.push(i);
        bound_out.push(o);
    }
    // Coder c's input fed by coder o's output: the library follows these
    // edges (recursively for multi-input coders), so they must not loop.
    let edges: Vec<(usize, usize)> = bound_in
        .iter()
        .zip(&bound_out)
        .map(|(&i, &o)| (owner[i as usize] as usize, o as usize))
        .collect();
    if has_cycle(nc as usize, &edges) {
        return bad("bad bind pairs");
    }
    let packed = (total_in - pairs) as usize;
    if packed > 1 {
        let mut seen = Vec::new();
        for _ in 0..packed {
            let i = p.num()?;
            if i >= total_in || bound_in.contains(&i) || seen.contains(&i) {
                return bad("bad packed streams");
            }
            seen.push(i);
        }
    }
    Ok((
        Folder {
            coders,
            packed,
            unpack_sizes: Vec::new(),
            has_crc: false,
            crc: 0,
            sub_streams: 1,
        },
        bound_out,
    ))
}

/// Whether the directed graph on `n` nodes has a cycle (n ≤ MAX_CODERS).
fn has_cycle(n: usize, edges: &[(usize, usize)]) -> bool {
    // Kahn: repeatedly remove nodes without incoming edges.
    let mut indegree = vec![0usize; n];
    for &(_, to) in edges {
        indegree[to] += 1;
    }
    let mut ready: Vec<usize> = (0..n).filter(|&v| indegree[v] == 0).collect();
    let mut removed = 0;
    while let Some(v) = ready.pop() {
        removed += 1;
        for &(from, to) in edges {
            if from == v {
                indegree[to] -= 1;
                if indegree[to] == 0 {
                    ready.push(to);
                }
            }
        }
    }
    removed < n
}

fn check_dictionaries(f: &Folder) -> Result<(), Problem> {
    for (i, c) in f.coders.iter().enumerate() {
        let dict = if c.id == ID_LZMA {
            if c.props.len() < 5 {
                return bad("bad LZMA properties");
            }
            // The LZMA decoder never allocates more than the output size.
            le(&c.props[1..5]).min(f.unpack_sizes[i])
        } else if c.id == ID_LZMA2 {
            let b = *c
                .props
                .first()
                .ok_or_else(|| Problem::Bad("bad LZMA2 properties".into()))?;
            match b {
                0..=39 => (2 | u64::from(b & 1)) << (b / 2 + 11),
                40 => u64::from(u32::MAX),
                _ => return bad("bad LZMA2 properties"),
            }
        } else {
            continue;
        };
        if dict > MAX_DICT {
            return Err(Problem::Bad(format!(
                "dictionary too large (limit {} MiB)",
                MAX_DICT >> 20
            )));
        }
    }
    Ok(())
}

fn sub_streams(p: &mut P<'_>, caps: Caps, s: &mut Streams) -> Result<(), Problem> {
    for (f, _) in s.folders.iter_mut() {
        f.sub_streams = 1;
    }
    let mut total = s.folders.len();
    let mut nid = p.u8()?;
    if nid == 0x0D {
        total = 0;
        for (f, _) in s.folders.iter_mut() {
            let n = caps.files(p.num()?)?;
            f.sub_streams = n;
            total = total.saturating_add(n);
        }
        caps.files(total as u64)?;
        nid = p.u8()?;
    }
    if nid == 0x09 {
        for (f, bound) in &s.folders {
            if f.sub_streams == 0 {
                continue;
            }
            let mut sum = 0u64;
            for _ in 0..f.sub_streams - 1 {
                sum = sum
                    .checked_add(p.num()?)
                    .ok_or_else(|| Problem::Bad("bad stream sizes".into()))?;
            }
            if sum > f.unpack_size(bound) {
                return bad("bad stream sizes");
            }
        }
        nid = p.u8()?;
    }
    let digests: usize = s
        .folders
        .iter()
        .filter(|(f, _)| f.sub_streams != 1 || !f.has_crc)
        .map(|(f, _)| f.sub_streams)
        .sum();
    if nid == 0x0A {
        let defined = p.all_or_bits(digests)?;
        p.bytes(4 * popcount(&defined) as u64)?;
        nid = p.u8()?;
    }
    if nid != 0x00 {
        return bad("bad substreams info");
    }
    s.sub_total = Some(total);
    Ok(())
}

/// The plain header database, after its `kHeader` byte.
fn header(p: &mut P<'_>, caps: Caps) -> Result<(), Problem> {
    let mut nid = p.u8()?;
    if nid == 0x02 {
        loop {
            if p.u8()? == 0 {
                break;
            }
            let n = p.num()?;
            p.bytes(n)?;
        }
        nid = p.u8()?;
    }
    if nid == 0x03 {
        return bad("additional streams");
    }
    let mut s = Streams::default();
    if nid == 0x04 {
        s = streams(p, caps)?;
        nid = p.u8()?;
    }
    let mut have_files = false;
    if nid == 0x05 {
        files(p, caps, &s)?;
        have_files = true;
        nid = p.u8()?;
    }
    if nid != 0x00 {
        return bad("bad header");
    }
    if !have_files && !s.folders.is_empty() {
        return bad("streams without files");
    }
    Ok(())
}

fn files(p: &mut P<'_>, caps: Caps, s: &Streams) -> Result<(), Problem> {
    let n = caps.files(p.num()?)?;
    let mut empty_stream: Option<Vec<bool>> = None;
    loop {
        let t = p.u8()?;
        if t == 0 {
            break;
        }
        let size = p.num()?;
        match t {
            0x0E => empty_stream = Some(p.bits(n)?),
            0x0F | 0x10 => {
                let k = empty_stream
                    .as_deref()
                    .map(popcount)
                    .ok_or_else(|| Problem::Bad("bad files info".into()))?;
                p.bits(k)?;
            }
            0x11 => {
                if p.u8()? != 0 || size == 0 || (size - 1) % 2 != 0 {
                    return bad("bad file names");
                }
                // The library indexes its file list by name position.
                if count_names(p.bytes(size - 1)?) != n {
                    return bad("file name count mismatch");
                }
            }
            0x12..=0x15 => {
                let defined = p.all_or_bits(n)?;
                if p.u8()? != 0 {
                    return bad("external file properties");
                }
                let width = if t == 0x15 { 4 } else { 8 };
                p.bytes(width * popcount(&defined) as u64)?;
            }
            0x18 => return bad("unsupported file property"),
            _ => {
                p.bytes(size)?;
            }
        }
    }
    let with_stream = n - empty_stream.as_deref().map(popcount).unwrap_or(0);
    match s.sub_total {
        Some(total) if total != with_stream => bad("stream count mismatch"),
        None if with_stream > 0 => bad("files without streams info"),
        _ => Ok(()),
    }
}

/// How many names `sevenz-rust2`'s name reader yields from `b` (UTF-16LE,
/// NUL-terminated; a last name may run to the end). `b.len()` is even.
fn count_names(b: &[u8]) -> usize {
    let mut count = 0;
    let mut i = 0;
    while i < b.len() {
        count += 1;
        while i + 1 < b.len() {
            let u = u16::from_le_bytes([b[i], b[i + 1]]);
            i += 2;
            if u == 0 {
                break;
            }
        }
        if i + 1 == b.len() {
            i += 1;
        }
    }
    count
}

/// Decode a compressed header (after its `kEncodedHeader` byte). Only the
/// forms 7-Zip writes are accepted: one folder of one COPY, LZMA or LZMA2
/// coder; AES anywhere means an encrypted header.
fn encoded_header(
    p: &mut P<'_>,
    mut file: &File,
    caps: Caps,
    budget: &Budget,
) -> Result<Vec<u8>, Problem> {
    // The folder count of an encoded header's streams is not a file count.
    let caps_any = Caps {
        remaining: caps.remaining.max(1),
        ..caps
    };
    let s = streams(p, caps_any)?;
    let Some((f, bound)) = s.folders.first() else {
        return bad("no header folder");
    };
    if f.coders.iter().any(|c| c.id == ID_AES) {
        return Err(Problem::Encrypted);
    }
    let [coder] = f.coders.as_slice() else {
        return bad("unsupported header coding");
    };
    let size = f.unpack_size(bound);
    if size > MAX_HEADER {
        return bad("header too large");
    }
    let pack_size = s.pack_sizes.as_deref().and_then(|v| v.first().copied());
    let Some(pack_size) = pack_size else {
        return bad("no header pack stream");
    };
    file.seek(SeekFrom::Start(SIGNATURE_HEADER + s.pack_pos))
        .map_err(Problem::Io)?;
    let input = io::BufReader::new(file.take(pack_size));
    let mut dec: Box<dyn Read + '_> = if coder.id == ID_COPY {
        Box::new(input)
    } else if coder.id == ID_LZMA {
        let props = &coder.props;
        let r = lzma_rust2::LzmaReader::new_with_props(
            input,
            size,
            props[0],
            le(&props[1..5]) as u32,
            None,
        )
        .map_err(|e| Problem::Bad(e.to_string()))?;
        Box::new(r)
    } else if coder.id == ID_LZMA2 {
        let b = coder.props[0];
        let dict = if b == 40 {
            u32::MAX
        } else {
            (2 | u32::from(b & 1)) << (b / 2 + 11)
        };
        Box::new(lzma_rust2::Lzma2Reader::new(input, dict, None))
    } else {
        return bad("unsupported header coding");
    };
    let mut out = Vec::new();
    let mut buf = vec![0u8; 64 * 1024];
    while (out.len() as u64) < size {
        let want = (size - out.len() as u64).min(buf.len() as u64) as usize;
        let n = dec
            .read(&mut buf[..want])
            .map_err(|e| Problem::Bad(e.to_string()))?;
        if n == 0 {
            return bad("truncated header");
        }
        budget.add_bytes(n as u64).map_err(Problem::Abort)?;
        out.extend_from_slice(&buf[..n]);
    }
    if f.has_crc && u64::from(crc32fast::hash(&out)) != f.crc {
        return bad("header checksum mismatch");
    }
    Ok(out)
}
