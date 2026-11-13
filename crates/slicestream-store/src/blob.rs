//! Streaming temp blobs and a content-addressed filesystem blob store.
//!
//! Layout under the root: `tmp/<random>.part` for in-flight spools and
//! `objects/aa/bb/<sha256-hex>.<ext>` for committed blobs, where `<ext>` comes
//! from the [`BlobKind`] (`dcm`, `nii`, `nii.gz`). Names are derived only from
//! random numbers, SHA-256 hex and the kind, never from external input.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use sha2::{Digest, Sha256};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

/// Chunk size used by [`FsBlobStore::spool_sync`].
const SYNC_CHUNK: usize = 64 * 1024;

/// SHA-256 digest identifying a blob.
#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub struct BlobHash(pub [u8; 32]);

impl BlobHash {
    /// Lowercase hex encoding (64 chars).
    pub fn to_hex(&self) -> String {
        use std::fmt::Write;
        let mut s = String::with_capacity(64);
        for b in self.0 {
            let _ = write!(s, "{b:02x}");
        }
        s
    }

    /// Parse 64 hex chars (either case); `None` otherwise.
    pub fn from_hex(s: &str) -> Option<BlobHash> {
        let b = s.as_bytes();
        if b.len() != 64 {
            return None;
        }
        let nib = |c: u8| match c {
            b'0'..=b'9' => Some(c - b'0'),
            b'a'..=b'f' => Some(c - b'a' + 10),
            b'A'..=b'F' => Some(c - b'A' + 10),
            _ => None,
        };
        let mut out = [0u8; 32];
        for (i, o) in out.iter_mut().enumerate() {
            *o = nib(b[2 * i])? << 4 | nib(b[2 * i + 1])?;
        }
        Some(BlobHash(out))
    }
}

/// What a committed blob holds; selects its file extension. Blobs are stored
/// exactly as received.
#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub enum BlobKind {
    Dicom,
    Nifti,
    NiftiGz,
}

impl BlobKind {
    /// Every kind, longest extension first (so `nii.gz` is tried before `nii`).
    const ALL: [BlobKind; 3] = [BlobKind::NiftiGz, BlobKind::Nifti, BlobKind::Dicom];

    /// File extension without the leading dot.
    pub fn ext(self) -> &'static str {
        match self {
            BlobKind::Dicom => "dcm",
            BlobKind::Nifti => "nii",
            BlobKind::NiftiGz => "nii.gz",
        }
    }

    /// Parse a committed blob's file name: exactly `<64 lowercase hex>.<ext>`,
    /// i.e. a name [`FsBlobStore::path_of`] could have produced.
    fn parse_file_name(name: &str) -> Option<(BlobHash, BlobKind)> {
        let (stem, ext) = name.split_once('.')?;
        let kind = BlobKind::ALL.into_iter().find(|k| k.ext() == ext)?;
        let hash = BlobHash::from_hex(stem)?;
        (hash.to_hex() == stem).then_some((hash, kind))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SpoolError {
    #[error("blob exceeds limit of {limit} bytes")]
    TooLarge { limit: u64 },
    #[error("io error: {0}")]
    Io(#[from] io::Error),
}

/// An in-progress spool file. Removed on drop unless finished.
#[derive(Debug)]
pub struct TempBlob {
    file: Option<File>,
    path: PathBuf,
    hasher: Sha256,
    len: u64,
    limit: u64,
    finished: bool,
}

impl TempBlob {
    pub async fn write_chunk(&mut self, buf: &[u8]) -> Result<(), SpoolError> {
        let new_len = self.len.saturating_add(buf.len() as u64);
        if new_len > self.limit {
            return Err(SpoolError::TooLarge { limit: self.limit });
        }
        let file = self.file.as_mut().expect("file present until finish");
        file.write_all(buf).await?;
        self.hasher.update(buf);
        self.len = new_len;
        Ok(())
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Flush, fsync and hand over as a [`SpooledBlob`].
    pub async fn finish(mut self) -> Result<SpooledBlob, SpoolError> {
        let mut file = self.file.take().expect("file present until finish");
        file.flush().await?;
        file.sync_all().await?;
        drop(file);
        self.finished = true;
        let hash = BlobHash(self.hasher.clone().finalize().into());
        Ok(SpooledBlob {
            hash,
            len: self.len,
            path: std::mem::take(&mut self.path),
            committed: false,
        })
    }
}

impl Drop for TempBlob {
    fn drop(&mut self) {
        if !self.finished {
            // Close the handle before removing; ignore errors (never panic).
            self.file = None;
            let _ = fs::remove_file(&self.path);
        }
    }
}

/// A fully written, fsynced temp file awaiting [`FsBlobStore::commit`].
/// Removed on drop unless committed.
#[derive(Debug)]
pub struct SpooledBlob {
    pub hash: BlobHash,
    pub len: u64,
    path: PathBuf,
    committed: bool,
}

impl SpooledBlob {
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for SpooledBlob {
    fn drop(&mut self) {
        if !self.committed {
            let _ = fs::remove_file(&self.path);
        }
    }
}

/// Content-addressed blob store on a local filesystem. Cheap to clone and
/// share across threads (the root is behind an `Arc`).
#[derive(Clone, Debug)]
pub struct FsBlobStore {
    root: Arc<PathBuf>,
}

impl FsBlobStore {
    pub fn open(root: &Path) -> io::Result<Self> {
        fs::create_dir_all(root.join("tmp"))?;
        fs::create_dir_all(root.join("objects"))?;
        Ok(Self {
            root: Arc::new(root.to_path_buf()),
        })
    }

    pub fn create_temp(&self, limit: u64) -> io::Result<TempBlob> {
        let id: u128 = rand::random();
        let path = self.root.join("tmp").join(format!("{id:032x}.part"));
        let file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        Ok(TempBlob {
            file: Some(File::from_std(file)),
            path,
            hasher: Sha256::new(),
            len: 0,
            limit,
            finished: false,
        })
    }

    /// Blocking twin of [`TempBlob`]: copy `reader` into a new spool file in
    /// 64 KiB chunks, hashing as it goes, then fsync. Fails with
    /// [`SpoolError::TooLarge`] once more than `limit` bytes are read. On any
    /// error the spool file is removed. Never holds more than one chunk in
    /// memory.
    pub fn spool_sync(
        &self,
        reader: &mut dyn io::Read,
        limit: u64,
    ) -> Result<SpooledBlob, SpoolError> {
        use std::io::Write;
        let id: u128 = rand::random();
        let path = self.root.join("tmp").join(format!("{id:032x}.part"));
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        // Removes the spool file on any early return. Created only after the
        // `create_new` open succeeded, so it never removes someone else's file.
        // (It drops before `file`; unlinking an open file is fine on Unix.)
        let mut guard = SpooledBlob {
            hash: BlobHash([0; 32]),
            len: 0,
            path,
            committed: false,
        };
        let mut hasher = Sha256::new();
        let mut buf = vec![0u8; SYNC_CHUNK];
        loop {
            let n = match reader.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => n,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e.into()),
            };
            let new_len = guard.len.saturating_add(n as u64);
            if new_len > limit {
                return Err(SpoolError::TooLarge { limit });
            }
            file.write_all(&buf[..n])?;
            hasher.update(&buf[..n]);
            guard.len = new_len;
        }
        file.flush()?;
        file.sync_all()?;
        drop(file);
        guard.hash = BlobHash(hasher.finalize().into());
        Ok(guard)
    }

    /// Atomically move a spooled blob into `objects/` as `kind` and fsync the
    /// directory. Blocking: call from `spawn_blocking` in async code.
    pub fn commit(&self, mut spooled: SpooledBlob, kind: BlobKind) -> io::Result<BlobHash> {
        let hash = spooled.hash;
        let target = self.path_of(&hash, kind);
        let parent = target.parent().expect("target has parent");
        if target.is_file() {
            // Another committer may have renamed but not yet fsynced; make the
            // entry durable before we report success. Dropping `spooled`
            // removes our temp file.
            sync_ancestors(parent)?;
            return Ok(hash);
        }
        fs::create_dir_all(parent)?;
        fs::rename(&spooled.path, &target)?;
        spooled.committed = true;
        sync_ancestors(parent)?;
        Ok(hash)
    }

    pub fn path_of(&self, hash: &BlobHash, kind: BlobKind) -> PathBuf {
        let hex = hash.to_hex();
        self.root
            .join("objects")
            .join(&hex[0..2])
            .join(&hex[2..4])
            .join(format!("{hex}.{}", kind.ext()))
    }

    /// Remove the blob `hash` of `kind`; other kinds with the same hash are
    /// untouched. A missing file is not an error.
    pub fn delete(&self, hash: &BlobHash, kind: BlobKind) -> io::Result<()> {
        match fs::remove_file(self.path_of(hash, kind)) {
            Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e),
            _ => Ok(()),
        }
    }

    /// Remove every file in `tmp/`; returns how many were removed. Files
    /// that vanish during the sweep are skipped, not treated as errors.
    pub fn sweep_tmp(&self) -> io::Result<usize> {
        let mut n = 0;
        for entry in fs::read_dir(self.root.join("tmp"))? {
            let entry = entry?;
            let is_file = match entry.file_type() {
                Ok(ft) => ft.is_file(),
                Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e),
            };
            if is_file {
                match fs::remove_file(entry.path()) {
                    Ok(()) => n += 1,
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e),
                }
            }
        }
        Ok(n)
    }

    /// Enumerate all committed blobs with their kinds. Files whose names are
    /// not `<64 hex>.<known ext>` are ignored.
    pub fn list(&self) -> io::Result<Vec<(BlobHash, BlobKind)>> {
        let mut out = Vec::new();
        for a in read_dirs(&self.root.join("objects"))? {
            for b in read_dirs(&a)? {
                for entry in fs::read_dir(&b)? {
                    let entry = entry?;
                    if let Some(parsed) = entry
                        .file_name()
                        .to_str()
                        .and_then(BlobKind::parse_file_name)
                    {
                        out.push(parsed);
                    }
                }
            }
        }
        Ok(out)
    }
}

fn read_dirs(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut v = Vec::new();
    for e in fs::read_dir(dir)? {
        let e = e?;
        if e.file_type()?.is_dir() {
            v.push(e.path());
        }
    }
    Ok(v)
}

/// fsync `dir` and its two ancestors (`aa/bb`, `aa`, `objects`).
fn sync_ancestors(dir: &Path) -> io::Result<()> {
    sync_dir(dir)?;
    if let Some(p) = dir.parent() {
        sync_dir(p)?;
        if let Some(pp) = p.parent() {
            sync_dir(pp)?;
        }
    }
    Ok(())
}

/// fsync a directory so a rename into it is durable. Unix only: Windows
/// cannot open a directory as a `File` this way (and Unix is the supported
/// platform), so elsewhere this is a no-op.
#[cfg(unix)]
fn sync_dir(dir: &Path) -> io::Result<()> {
    fs::File::open(dir)?.sync_all()
}

#[cfg(not(unix))]
fn sync_dir(_dir: &Path) -> io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn count_files(dir: &std::path::Path) -> usize {
        std::fs::read_dir(dir).unwrap().count()
    }

    async fn spool(store: &FsBlobStore, data: &[u8]) -> SpooledBlob {
        let mut t = store.create_temp(1024).unwrap();
        t.write_chunk(data).await.unwrap();
        t.finish().await.unwrap()
    }

    #[tokio::test]
    async fn spool_hash_matches_sha256() {
        let d = tempfile::tempdir().unwrap();
        let store = FsBlobStore::open(d.path()).unwrap();
        let mut t = store.create_temp(1024).unwrap();
        t.write_chunk(b"hel").await.unwrap();
        t.write_chunk(b"lo").await.unwrap();
        let s = t.finish().await.unwrap();
        assert_eq!(
            s.hash.to_hex(),
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
        assert_eq!(s.len, 5);
        assert_eq!(BlobHash::from_hex(&s.hash.to_hex()), Some(s.hash));
    }

    #[tokio::test]
    async fn limit_enforced() {
        let d = tempfile::tempdir().unwrap();
        let store = FsBlobStore::open(d.path()).unwrap();
        let mut t = store.create_temp(4).unwrap();
        let err = t.write_chunk(b"12345").await.unwrap_err();
        assert!(matches!(err, SpoolError::TooLarge { limit: 4 }));
    }

    #[tokio::test]
    async fn drop_removes_temp() {
        let d = tempfile::tempdir().unwrap();
        let store = FsBlobStore::open(d.path()).unwrap();
        let mut t = store.create_temp(1024).unwrap();
        t.write_chunk(b"abc").await.unwrap();
        drop(t);
        assert_eq!(count_files(&d.path().join("tmp")), 0);
        let s = spool(&store, b"abc").await;
        assert_eq!(count_files(&d.path().join("tmp")), 1);
        drop(s);
        assert_eq!(count_files(&d.path().join("tmp")), 0);
    }

    #[tokio::test]
    async fn commit_is_idempotent() {
        let d = tempfile::tempdir().unwrap();
        let store = FsBlobStore::open(d.path()).unwrap();
        let a = spool(&store, b"same").await;
        let h1 = store.commit(a, BlobKind::Dicom).unwrap();
        let b = spool(&store, b"same").await;
        let h2 = store.commit(b, BlobKind::Dicom).unwrap();
        assert_eq!(h1, h2);
        assert!(store.path_of(&h1, BlobKind::Dicom).is_file());
        assert_eq!(store.list().unwrap(), vec![(h1, BlobKind::Dicom)]);
        assert_eq!(count_files(&d.path().join("tmp")), 0);
        store.delete(&h1, BlobKind::Dicom).unwrap();
        store.delete(&h1, BlobKind::Dicom).unwrap();
        assert!(store.list().unwrap().is_empty());
    }

    #[tokio::test]
    async fn commit_existing_target_returns_hash_and_cleans_temp() {
        let d = tempfile::tempdir().unwrap();
        let store = FsBlobStore::open(d.path()).unwrap();
        let h1 = store
            .commit(spool(&store, b"dup").await, BlobKind::Dicom)
            .unwrap();
        let b = spool(&store, b"dup").await;
        let tmp = b.path().to_path_buf();
        assert!(tmp.exists());
        assert_eq!(store.commit(b, BlobKind::Dicom).unwrap(), h1);
        assert!(!tmp.exists());
        assert!(store.path_of(&h1, BlobKind::Dicom).is_file());
    }

    #[tokio::test]
    async fn sweep_and_list() {
        let d = tempfile::tempdir().unwrap();
        let store = FsBlobStore::open(d.path()).unwrap();
        let h = store
            .commit(spool(&store, b"x").await, BlobKind::Dicom)
            .unwrap();
        std::fs::write(d.path().join("tmp/x.part"), b"junk").unwrap();
        assert_eq!(store.sweep_tmp().unwrap(), 1);
        assert_eq!(store.list().unwrap(), vec![(h, BlobKind::Dicom)]);
    }

    #[tokio::test]
    async fn nifti_kinds_have_own_extension() {
        let d = tempfile::tempdir().unwrap();
        let store = FsBlobStore::open(d.path()).unwrap();
        let h = store
            .commit(spool(&store, b"nii-gz bytes").await, BlobKind::NiftiGz)
            .unwrap();
        let hex = h.to_hex();
        let p = store.path_of(&h, BlobKind::NiftiGz);
        assert!(p.ends_with(format!(
            "objects/{}/{}/{hex}.nii.gz",
            &hex[0..2],
            &hex[2..4]
        )));
        assert!(p.is_file());
        assert!(
            store
                .path_of(&h, BlobKind::Nifti)
                .to_str()
                .unwrap()
                .ends_with(&format!("{hex}.nii"))
        );
        assert!(
            store
                .path_of(&h, BlobKind::Dicom)
                .to_str()
                .unwrap()
                .ends_with(&format!("{hex}.dcm"))
        );
        assert!(!store.path_of(&h, BlobKind::Dicom).exists());
        assert_eq!(BlobKind::Dicom.ext(), "dcm");
        assert_eq!(BlobKind::Nifti.ext(), "nii");
        assert_eq!(BlobKind::NiftiGz.ext(), "nii.gz");
        store.delete(&h, BlobKind::Dicom).unwrap();
        assert!(
            p.is_file(),
            "deleting another kind must not touch this file"
        );
        store.delete(&h, BlobKind::NiftiGz).unwrap();
        assert!(!p.exists());
    }

    #[tokio::test]
    async fn list_returns_kinds() {
        let d = tempfile::tempdir().unwrap();
        let store = FsBlobStore::open(d.path()).unwrap();
        let a = store
            .commit(spool(&store, b"a").await, BlobKind::Dicom)
            .unwrap();
        let b = store
            .commit(spool(&store, b"b").await, BlobKind::Nifti)
            .unwrap();
        let c = store
            .commit(spool(&store, b"c").await, BlobKind::NiftiGz)
            .unwrap();
        // Same hash under two kinds: two entries.
        store
            .commit(spool(&store, b"a").await, BlobKind::Nifti)
            .unwrap();
        // Unknown names are ignored.
        let dir = store
            .path_of(&a, BlobKind::Dicom)
            .parent()
            .unwrap()
            .to_path_buf();
        std::fs::write(dir.join("notes.txt"), b"x").unwrap();
        std::fs::write(dir.join(format!("{}.gz", a.to_hex())), b"x").unwrap();
        std::fs::write(dir.join(format!("{}.dcm.bak", a.to_hex())), b"x").unwrap();
        std::fs::write(dir.join("abc.nii.gz"), b"x").unwrap();
        let upper = format!("{}.nii", a.to_hex().to_uppercase());
        std::fs::write(dir.join(upper), b"x").unwrap();
        let mut got = store.list().unwrap();
        got.sort_by_key(|(h, k)| (h.to_hex(), k.ext()));
        let mut want = vec![
            (a, BlobKind::Dicom),
            (a, BlobKind::Nifti),
            (b, BlobKind::Nifti),
            (c, BlobKind::NiftiGz),
        ];
        want.sort_by_key(|(h, k)| (h.to_hex(), k.ext()));
        assert_eq!(got, want);
    }

    #[test]
    fn spool_sync_hashes_and_limits() {
        let d = tempfile::tempdir().unwrap();
        let store = FsBlobStore::open(d.path()).unwrap();
        // Larger than one 64 KiB chunk.
        let data: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();
        let s = store.spool_sync(&mut &data[..], data.len() as u64).unwrap();
        let want: [u8; 32] = Sha256::digest(&data).into();
        assert_eq!(s.hash, BlobHash(want));
        assert_eq!(s.len, data.len() as u64);
        assert_eq!(std::fs::read(s.path()).unwrap(), data);
        drop(s);
        assert_eq!(count_files(&d.path().join("tmp")), 0);

        let err = store
            .spool_sync(&mut &data[..], data.len() as u64 - 1)
            .unwrap_err();
        assert!(matches!(err, SpoolError::TooLarge { limit } if limit == data.len() as u64 - 1));
        assert_eq!(count_files(&d.path().join("tmp")), 0);

        // Read errors also leave nothing behind.
        struct Failing;
        impl io::Read for Failing {
            fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
                Err(io::Error::other("boom"))
            }
        }
        assert!(matches!(
            store.spool_sync(&mut Failing, 10),
            Err(SpoolError::Io(_))
        ));
        assert_eq!(count_files(&d.path().join("tmp")), 0);
    }

    #[test]
    fn send_and_clone() {
        fn s<T: Send>() {}
        fn sc<T: Send + Sync + Clone>() {}
        s::<TempBlob>();
        s::<SpooledBlob>();
        sc::<FsBlobStore>();
    }
}
