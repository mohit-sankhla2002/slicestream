mod support;

use slicestream_archive::{Kind, Limits, WalkError};
use support::*;

/// Byte offset of the header database of a 7z file.
fn sz_next_header(b: &[u8]) -> usize {
    32 + u64::from_le_bytes(b[12..20].try_into().unwrap()) as usize
}

// ------------------------------------------------------------------ 7z

#[test]
fn seven_z_with_nested_tar_gz() {
    let im1 = dicom("IM1");
    let tgz = gz(&tar(&[TarItem::File("IM1", &im1)]));
    let s = seven_z(&[("d/", b""), ("inner.tar.gz", &tgz), ("notes.txt", b"hello")]);
    let (r, rec, e) = run(&s, "x.7z", &Limits::default());
    let stats = r.unwrap();
    assert_eq!(rec.paths(), vec!["x.7z!/inner.tar.gz!/IM1"]);
    let l = rec.leaf("x.7z!/inner.tar.gz!/IM1");
    assert_eq!(l.kind, Kind::Dicom);
    assert_eq!(l.sha, sha(&im1));
    assert_eq!(rec.skipped, vec!["x.7z!/notes.txt"]);
    assert!(rec.failed.is_empty() && rec.entry_failed.is_empty());
    // inner.tar.gz, notes.txt, IM1 (the directory is not an entry)
    assert_eq!(stats.entries_seen, 3);
    assert!(stats.bytes_expanded >= (tgz.len() + im1.len()) as u64);
    assert_tmp_empty(&e.tmp);
}

#[test]
fn seven_z_solid_block_counts_every_decoded_byte() {
    let a = dicom("A");
    let b = dicom("B");
    let junk = vec![0x55u8; 2 * 1024 * 1024];
    let s = seven_z_solid(&[("IM1", &a), ("junk.bin", &junk), ("IM2", &b)]);
    let (r, rec, e) = run(&s, "s.7z", &Limits::default());
    let stats = r.unwrap();
    assert_eq!(rec.paths(), vec!["s.7z!/IM1", "s.7z!/IM2"]);
    assert_eq!(rec.leaf("s.7z!/IM2").sha, sha(&b));
    assert_eq!(rec.skipped, vec!["s.7z!/junk.bin"]);
    // the skipped entry is decoded (and counted) to reach IM2
    assert!(stats.bytes_expanded >= (a.len() + junk.len() + b.len()) as u64);
    assert_tmp_empty(&e.tmp);

    // ...so it counts toward the limit even though it is never stored
    let limits = Limits {
        max_expanded_bytes: 1024 * 1024,
        ..Limits::default()
    };
    let (r, rec, e) = run(&s, "s.7z", &limits);
    match r {
        Err(WalkError::Aborted(m)) => assert!(m.contains("expanded size limit"), "{m}"),
        other => panic!("expected abort, got {other:?}"),
    }
    assert!(!rec.paths().contains(&"s.7z!/IM2"));
    assert_tmp_empty(&e.tmp);
}

#[test]
fn encrypted_7z_fails_alone() {
    let a = dicom("A");
    for encrypt_header in [true, false] {
        let enc = seven_z_encrypted(&[("IM9", &dicom("secret"))], encrypt_header);
        let z = zip(&[("enc.7z", &enc), ("IM1", &a)]);
        let (r, rec, e) = run(&z, "o.zip", &Limits::default());
        r.unwrap();
        assert_eq!(rec.paths(), vec!["o.zip!/IM1"], "header {encrypt_header}");
        assert_eq!(
            rec.failed,
            vec![("o.zip!/enc.7z".to_string(), "encrypted archive".to_string())],
            "header {encrypt_header}"
        );
        assert_tmp_empty(&e.tmp);
    }
}

#[test]
fn seven_z_corrupt_entry_fails_only_that_entry() {
    let a = dicom("AAAA");
    let b = dicom("BBBB");
    let s = corrupt(seven_z_stored(&[("IM1", &a), ("IM2", &b)]), b"AAAA");
    let (r, rec, e) = run(&s, "c.7z", &Limits::default());
    r.unwrap();
    assert_eq!(rec.paths(), vec!["c.7z!/IM2"]);
    assert_eq!(rec.entry_failed.len(), 1, "{:?}", rec.entry_failed);
    assert_eq!(rec.entry_failed[0].0, "c.7z!/IM1");
    assert!(rec.entry_failed[0].1.starts_with("cannot read archive"));
    assert!(rec.failed.is_empty());
    assert_tmp_empty(&e.tmp);
}

#[test]
fn seven_z_compressed_header_is_read() {
    let files: Vec<(String, Vec<u8>)> = (0..60)
        .map(|i| {
            (
                format!("series/long-directory-name/IM{i:04}"),
                dicom(&format!("{i}")),
            )
        })
        .collect();
    let refs: Vec<(&str, &[u8])> = files
        .iter()
        .map(|(n, d)| (n.as_str(), d.as_slice()))
        .collect();
    let s = seven_z(&refs);
    assert_eq!(
        s[sz_next_header(&s)],
        0x17,
        "fixture must have an encoded header"
    );
    let (r, rec, e) = run(&s, "h.7z", &Limits::default());
    assert_eq!(r.unwrap().entries_seen, 60);
    assert_eq!(rec.leaves.len(), 60);
    assert_eq!(
        rec.leaf("h.7z!/series/long-directory-name/IM0059").sha,
        sha(&files[59].1)
    );
    assert_tmp_empty(&e.tmp);
}

#[test]
fn seven_z_declared_entry_count_is_checked_before_parsing() {
    let s = seven_z(&[("a", b"1"), ("b", b"2"), ("c", b"3")]);
    let limits = Limits {
        max_entries: 2,
        ..Limits::default()
    };
    let (r, rec, e) = run(&s, "x.7z", &limits);
    match r {
        Err(WalkError::Aborted(m)) => assert_eq!(m, "too many entries (limit 2)"),
        other => panic!("expected abort, got {other:?}"),
    }
    assert!(rec.skipped.is_empty(), "nothing walked before the abort");
    assert_tmp_empty(&e.tmp);

    // a header claiming 2^40 files: refused, not allocated
    let mut h = vec![0x01, 0x05];
    h.extend(sz_num(1 << 40));
    h.extend([0x00, 0x00]);
    let (r, _rec, _e) = run(&seven_z_raw(&[], &h), "x.7z", &Limits::default());
    match r {
        Err(WalkError::Aborted(m)) => assert!(m.starts_with("too many entries"), "{m}"),
        other => panic!("expected abort, got {other:?}"),
    }
}

fn assert_7z_refused(bytes: &[u8], detail: &str) {
    let (r, rec, e) = run(bytes, "x.7z", &Limits::default());
    r.unwrap();
    assert_eq!(rec.failed.len(), 1, "{:?}", rec.failed);
    assert_eq!(rec.failed[0].0, "x.7z");
    assert_eq!(rec.failed[0].1, format!("cannot read archive: {detail}"));
    assert!(rec.leaves.is_empty());
    assert_tmp_empty(&e.tmp);
}

/// Declared counts beyond what the budget has left abort the upload (as
/// the ZIP directory pre-check does), before anything is allocated.
fn assert_7z_aborts(bytes: &[u8]) {
    let (r, rec, e) = run(bytes, "x.7z", &Limits::default());
    match r {
        Err(WalkError::Aborted(m)) => assert!(m.starts_with("too many entries"), "{m}"),
        other => panic!("expected abort, got {other:?}"),
    }
    assert!(rec.leaves.is_empty());
    assert_tmp_empty(&e.tmp);
}

#[test]
fn seven_z_hostile_headers_are_refused_without_allocating() {
    // declared header size over the cap (the bytes are not even there)
    assert_7z_refused(
        &seven_z_raw_sized(&[], &[0x01, 0x00], 300 * 1024 * 1024),
        "header too large",
    );

    // compressed header declaring a 1 TiB unpacked size
    let mut h = vec![0x17, 0x06, 0x00, 0x01, 0x09, 0x01, 0x00]; // pack info: 1 stream of 1 byte
    h.extend([0x07, 0x0B, 0x01, 0x00, 0x01, 0x01, 0x00]); // 1 folder, 1 coder: COPY
    h.push(0x0C);
    h.extend(sz_num(1 << 40));
    h.extend([0x00, 0x00]);
    assert_7z_refused(&seven_z_raw(&[0x01], &h), "header too large");

    // 2^40 pack streams
    let mut h = vec![0x01, 0x04, 0x06, 0x00];
    h.extend(sz_num(1 << 40));
    h.extend([0x09, 0x01, 0x00, 0x00, 0x00, 0x00]);
    assert_7z_aborts(&seven_z_raw(&[], &h));

    // 2^40 folders
    let mut h = vec![0x01, 0x04, 0x07, 0x0B];
    h.extend(sz_num(1 << 40));
    h.extend([0x00, 0x01, 0x01, 0x00, 0x0C, 0x01, 0x00, 0x00, 0x00]);
    assert_7z_aborts(&seven_z_raw(&[], &h));

    // 2^40 unpack streams in a folder
    let mut h = vec![0x01, 0x04, 0x06, 0x00, 0x01, 0x09, 0x01, 0x00];
    h.extend([0x07, 0x0B, 0x01, 0x00, 0x01, 0x01, 0x00, 0x0C, 0x01, 0x00]);
    h.extend([0x08, 0x0D]);
    h.extend(sz_num(1 << 40));
    h.extend([0x00, 0x00, 0x00]);
    assert_7z_aborts(&seven_z_raw(&[0x00], &h));

    // more names than files (the parser would index past its file list)
    let mut h = vec![0x01, 0x05, 0x01, 0x11, 0x07, 0x00];
    h.extend([b'a', 0, 0, 0, b'b', 0, 0, 0]);
    h.extend([0x00, 0x00]);
    h[4] = 9; // property size: external byte + 8 name bytes
    assert_7z_refused(&seven_z_raw(&[], &h), "file name count mismatch");

    // LZMA2 dictionary of 4 GiB
    let mut h = vec![0x01, 0x04, 0x06, 0x00, 0x01, 0x09, 0x01, 0x00];
    h.extend([
        0x07, 0x0B, 0x01, 0x00, 0x01, 0x21, 0x21, 0x01, 40, 0x0C, 0x01, 0x00,
    ]);
    h.extend([
        0x08, 0x00, 0x00, 0x05, 0x01, 0x11, 0x05, 0x00, b'a', 0, 0, 0, 0x00, 0x00,
    ]);
    assert_7z_refused(
        &seven_z_raw(&[0x00], &h),
        "dictionary too large (limit 256 MiB)",
    );

    // bind pairs forming a loop (in 1 <- out 0, in 2 <- out 1, in 1 <- out 2)
    let mut h = vec![0x01, 0x04, 0x06, 0x00, 0x01, 0x09, 0x01, 0x00];
    h.extend([0x07, 0x0B, 0x01, 0x00, 0x04]);
    h.extend([0x01, 0x00, 0x01, 0x00, 0x01, 0x00, 0x01, 0x00]); // 4 COPY coders
    h.extend([0x01, 0x00, 0x02, 0x01, 0x01, 0x02]);
    h.extend([0x0C, 0x01, 0x01, 0x01, 0x01, 0x00]);
    h.extend([
        0x08, 0x00, 0x00, 0x05, 0x01, 0x11, 0x05, 0x00, b'a', 0, 0, 0, 0x00, 0x00,
    ]);
    assert_7z_refused(&seven_z_raw(&[0x00], &h), "bad bind pairs");

    // truncated archive
    let s = seven_z(&[("IM1", &dicom("A"))]);
    assert_7z_refused(&s[..s.len() - 10], "truncated");
}

// ------------------------------------------------------------------ RAR

#[cfg(feature = "rar")]
#[test]
fn rar_with_dicom_and_nested_zip() {
    let a = dicom("A");
    let b = dicom("B");
    let inner = zip(&[("IM2", &b)]);
    let r5 = rar5(
        &[
            RarItem::File("IM1", &a),
            RarItem::Dir("d"),
            RarItem::File("d/inner.zip", &inner),
            RarItem::File("notes.txt", b"hello"),
            RarItem::FileCopy("copy-of-IM1", "IM1"),
        ],
        false,
    );
    let (r, rec, e) = run(&r5, "x.rar", &Limits::default());
    let stats = r.unwrap();
    assert_eq!(rec.paths(), vec!["x.rar!/IM1", "x.rar!/d/inner.zip!/IM2"]);
    assert_eq!(rec.leaf("x.rar!/IM1").sha, sha(&a));
    assert_eq!(rec.leaf("x.rar!/d/inner.zip!/IM2").sha, sha(&b));
    assert_eq!(rec.skipped, vec!["x.rar!/notes.txt"]);
    assert!(
        rec.failed.is_empty() && rec.entry_failed.is_empty(),
        "{:?}",
        rec.entry_failed
    );
    // IM1, inner.zip, notes.txt, IM2
    assert_eq!(stats.entries_seen, 4);
    assert!(stats.bytes_expanded >= (a.len() + inner.len() + b.len()) as u64);
    assert_tmp_empty(&e.tmp);
}

#[cfg(feature = "rar")]
#[test]
fn rar_solid_counts_skipped_entries() {
    let a = dicom("A");
    let junk = vec![1u8; 100_000];
    let r5 = rar5(
        &[RarItem::File("junk", &junk), RarItem::File("IM1", &a)],
        true,
    );
    let (r, rec, e) = run(&r5, "s.rar", &Limits::default());
    let stats = r.unwrap();
    assert_eq!(rec.paths(), vec!["s.rar!/IM1"]);
    assert!(stats.bytes_expanded >= (junk.len() + a.len()) as u64);
    assert_tmp_empty(&e.tmp);
}

#[cfg(feature = "rar")]
#[test]
fn rar_corrupt_entry_fails_only_that_entry() {
    let a = dicom("AAAA");
    let b = dicom("BBBB");
    let r5 = corrupt(
        rar5(&[RarItem::File("IM1", &a), RarItem::File("IM2", &b)], false),
        b"AAAA",
    );
    let (r, rec, e) = run(&r5, "c.rar", &Limits::default());
    r.unwrap();
    assert_eq!(rec.paths(), vec!["c.rar!/IM2"]);
    assert_eq!(rec.entry_failed.len(), 1, "{:?}", rec.entry_failed);
    assert_eq!(rec.entry_failed[0].0, "c.rar!/IM1");
    assert!(rec.entry_failed[0].1.starts_with("cannot read archive"));
    assert_tmp_empty(&e.tmp);
}

#[cfg(feature = "rar")]
#[test]
fn rar_encryption() {
    let a = dicom("A");
    let secret = dicom("S");
    // every file encrypted: the archive is
    let all = rar5(&[RarItem::Encrypted("IM9", &secret)], false);
    let (r, rec, e) = run(&all, "e.rar", &Limits::default());
    r.unwrap();
    assert_eq!(
        rec.failed,
        vec![("e.rar".to_string(), "encrypted archive".to_string())]
    );
    assert_tmp_empty(&e.tmp);

    // some encrypted: those entries
    let mixed = rar5(
        &[RarItem::Encrypted("IM9", &secret), RarItem::File("IM1", &a)],
        false,
    );
    let (r, rec, e) = run(&mixed, "m.rar", &Limits::default());
    r.unwrap();
    assert_eq!(rec.paths(), vec!["m.rar!/IM1"]);
    assert_eq!(
        rec.entry_failed,
        vec![("m.rar!/IM9".to_string(), "encrypted archive".to_string())]
    );
    assert_tmp_empty(&e.tmp);

    // encrypted headers
    let z = zip(&[("h.rar", &rar5_encrypted_headers()), ("IM1", &a)]);
    let (r, rec, e) = run(&z, "o.zip", &Limits::default());
    r.unwrap();
    assert_eq!(rec.paths(), vec!["o.zip!/IM1"]);
    assert_eq!(
        rec.failed,
        vec![("o.zip!/h.rar".to_string(), "encrypted archive".to_string())]
    );
    assert_tmp_empty(&e.tmp);
}

#[cfg(feature = "rar")]
#[test]
fn rar_limits_and_truncation() {
    let a = dicom("A");
    let r5 = rar5(
        &[
            RarItem::File("IM1", &a),
            RarItem::File("IM2", &a),
            RarItem::File("IM3", &a),
        ],
        false,
    );
    let limits = Limits {
        max_entries: 2,
        ..Limits::default()
    };
    let (r, rec, e) = run(&r5, "x.rar", &limits);
    match r {
        Err(WalkError::Aborted(m)) => assert_eq!(m, "too many entries (limit 2)"),
        other => panic!("expected abort, got {other:?}"),
    }
    assert!(rec.leaves.is_empty(), "declared entries are checked first");
    assert_tmp_empty(&e.tmp);

    let big = vec![7u8; 300_000];
    let r5 = rar5(&[RarItem::File("big", &big)], false);
    let limits = Limits {
        max_expanded_bytes: 100_000,
        ..Limits::default()
    };
    let (r, _rec, e) = run(&r5, "x.rar", &limits);
    match r {
        Err(WalkError::Aborted(m)) => assert!(m.contains("expanded size limit"), "{m}"),
        other => panic!("expected abort, got {other:?}"),
    }
    assert_tmp_empty(&e.tmp);

    let r5 = rar5(&[RarItem::File("IM1", &a)], false);
    let (r, rec, e) = run(&r5[..r5.len() - 40], "t.rar", &Limits::default());
    r.unwrap();
    assert!(rec.leaves.is_empty());
    assert_eq!(
        rec.failed.len() + rec.entry_failed.len(),
        1,
        "{:?} {:?}",
        rec.failed,
        rec.entry_failed
    );
    assert_tmp_empty(&e.tmp);
}

#[cfg(feature = "rar")]
#[test]
fn zip_inside_rar_inside_7z() {
    let a = dicom("A");
    let z = zip(&[("IM1", &a)]);
    let r5 = rar5(&[RarItem::File("inner.zip", &z)], false);
    let s = seven_z(&[("outer.rar", &r5)]);
    let (r, rec, e) = run(&s, "x.7z", &Limits::default());
    r.unwrap();
    assert_eq!(rec.paths(), vec!["x.7z!/outer.rar!/inner.zip!/IM1"]);
    assert_eq!(rec.leaf("x.7z!/outer.rar!/inner.zip!/IM1").sha, sha(&a));
    assert_tmp_empty(&e.tmp);
}

#[cfg(not(feature = "rar"))]
#[test]
fn rar_without_feature_reports_not_built_in() {
    let r5 = rar5(&[RarItem::File("IM1", &dicom("A"))], false);
    let (r, rec, e) = run(&r5, "x.rar", &Limits::default());
    r.unwrap();
    assert_eq!(
        rec.failed,
        vec![("x.rar".to_string(), "RAR support not built in".to_string())]
    );
    assert_tmp_empty(&e.tmp);
}
