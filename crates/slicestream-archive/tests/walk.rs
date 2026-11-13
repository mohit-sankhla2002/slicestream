mod support;

use std::io;

use slicestream_archive::{
    Event, Kind, Leaf, Limits, Sink, WalkError, WalkStats, walk_file, walk_file_continue,
};
use support::*;

#[test]
fn limits_default_matches_spec() {
    let l = Limits::default();
    assert_eq!(l.max_depth, 4);
    assert_eq!(l.max_entries, 200_000);
    assert_eq!(l.max_expanded_bytes, 68_719_476_736);
    assert_eq!(l.min_free_bytes, 2_147_483_648);
}

#[test]
fn single_zip_finds_dicom_without_extension() {
    let im1 = dicom("IM1");
    let z = zip(&[("DIR/", b""), ("DIR/IM1", &im1)]);
    let (r, rec, e) = run(&z, "study.zip", &Limits::default());
    let stats = r.unwrap();
    assert_eq!(rec.paths(), vec!["study.zip!/DIR/IM1"]);
    let l = rec.leaf("study.zip!/DIR/IM1");
    assert_eq!(l.kind, Kind::Dicom);
    assert!(!l.compressed);
    assert_eq!(l.sha, sha(&im1));
    assert_eq!(l.head_len, im1.len());
    assert!(rec.skipped.is_empty(), "directories are not skipped events");
    assert_eq!(stats.entries_seen, 1);
    assert!(stats.bytes_expanded >= im1.len() as u64);
    assert_tmp_empty(&e.tmp);
}

#[test]
fn zip_of_zips_paths() {
    let a = dicom("A");
    let b = dicom("B");
    let p1 = zip(&[("IM1", &a)]);
    let p2 = zip(&[("sub/IM2", &b)]);
    let outer = zip(&[("p1.zip", &p1), ("p2.zip", &p2)]);
    let (r, rec, e) = run(&outer, "outer.zip", &Limits::default());
    let stats = r.unwrap();
    assert_eq!(
        rec.paths(),
        vec!["outer.zip!/p1.zip!/IM1", "outer.zip!/p2.zip!/sub/IM2"]
    );
    assert_eq!(rec.leaf("outer.zip!/p1.zip!/IM1").sha, sha(&a));
    assert_eq!(rec.leaf("outer.zip!/p2.zip!/sub/IM2").sha, sha(&b));
    assert_eq!(stats.entries_seen, 4);
    // inner zips counted once as spooled bytes and again for their contents
    let min = (p1.len() + p2.len() + a.len() + b.len()) as u64;
    assert!(
        stats.bytes_expanded >= min,
        "{} < {min}",
        stats.bytes_expanded
    );
    assert_tmp_empty(&e.tmp);
}

#[test]
fn tar_gz_and_plain_tar() {
    let a = dicom("A");
    let b = dicom("B");
    let t = tar(&[
        TarItem::Dir("d"),
        TarItem::File("d/IM1", &a),
        TarItem::Symlink("d/link", "/etc/passwd"),
        TarItem::File("d/IM2", &b),
    ]);
    let (r, rec, e) = run(&t, "x.tar", &Limits::default());
    assert_eq!(r.unwrap().entries_seen, 2);
    assert_eq!(rec.paths(), vec!["x.tar!/d/IM1", "x.tar!/d/IM2"]);
    assert_eq!(rec.leaf("x.tar!/d/IM2").sha, sha(&b));
    assert!(rec.skipped.is_empty() && rec.failed.is_empty());
    assert_tmp_empty(&e.tmp);

    let tgz = gz(&t);
    let (r, rec, e) = run(&tgz, "x.tar.gz", &Limits::default());
    let stats = r.unwrap();
    assert_eq!(stats.entries_seen, 2);
    assert_eq!(rec.paths(), vec!["x.tar.gz!/d/IM1", "x.tar.gz!/d/IM2"]);
    assert_eq!(rec.leaf("x.tar.gz!/d/IM1").sha, sha(&a));
    // the decoder output is counted, headers included (the end-of-archive
    // zero blocks need not be read)
    assert!(stats.bytes_expanded >= t.len() as u64 - 1024);
    assert_tmp_empty(&e.tmp);

    // tar.gz nested inside a zip
    let z = zip(&[("a.tgz", &tgz)]);
    let (r, rec, e) = run(&z, "o.zip", &Limits::default());
    r.unwrap();
    assert_eq!(
        rec.paths(),
        vec!["o.zip!/a.tgz!/d/IM1", "o.zip!/a.tgz!/d/IM2"]
    );
    assert_tmp_empty(&e.tmp);
}

#[test]
fn gz_nifti_is_compressed_leaf() {
    let n = nifti1();
    let g = gz(&n);
    let z = zip(&[("brain.nii.gz", &g)]);
    let (r, rec, e) = run(&z, "o.zip", &Limits::default());
    r.unwrap();
    let l = rec.leaf("o.zip!/brain.nii.gz");
    assert_eq!(l.kind, Kind::Nifti);
    assert!(l.compressed);
    assert_eq!(l.sha, sha(&g), "sink receives the raw gz bytes");
    assert_eq!(l.head_len, n.len(), "head is decompressed");
    assert_tmp_empty(&e.tmp);

    // top-level .nii.gz
    let (r, rec, _e) = run(&g, "brain.nii.gz", &Limits::default());
    r.unwrap();
    let l = rec.leaf("brain.nii.gz");
    assert!(l.compressed);
    assert_eq!(l.sha, sha(&g));
}

#[test]
fn gz_dicom_is_decompressed_leaf() {
    let d = dicom("GZ");
    let (r, rec, _e) = run(&gz(&d), "im.dcm.gz", &Limits::default());
    r.unwrap();
    let l = rec.leaf("im.dcm.gz");
    assert_eq!(l.kind, Kind::Dicom);
    assert!(!l.compressed);
    assert_eq!(l.sha, sha(&d));
}

#[test]
fn plain_nifti_leaf() {
    let n = nifti1();
    let (r, rec, _e) = run(&n, "brain.nii", &Limits::default());
    r.unwrap();
    let l = rec.leaf("brain.nii");
    assert_eq!(l.kind, Kind::Nifti);
    assert!(!l.compressed);
    assert_eq!(l.sha, sha(&n));

    let z = zip(&[("x/brain.nii", &n)]);
    let (r, rec, _e) = run(&z, "o.zip", &Limits::default());
    r.unwrap();
    let l = rec.leaf("o.zip!/x/brain.nii");
    assert_eq!(l.kind, Kind::Nifti);
    assert_eq!(l.sha, sha(&n));
}

#[test]
fn bare_dicom_top_level() {
    let d = dicom("BARE");
    let (r, rec, e) = run(&d, "IM0001", &Limits::default());
    let stats = r.unwrap();
    assert_eq!(rec.paths(), vec!["IM0001"]);
    assert_eq!(rec.leaf("IM0001").sha, sha(&d));
    assert_eq!(stats.entries_seen, 1);
    assert_tmp_empty(&e.tmp);

    let (r, rec, _e) = run(b"just some text", "notes.txt", &Limits::default());
    r.unwrap();
    assert!(rec.leaves.is_empty());
    assert_eq!(rec.skipped, vec!["notes.txt"]);
}

#[test]
fn messy_export() {
    let im1 = dicom("IM1");
    let mut exe = b"MZ\x90\x00".to_vec();
    exe.extend_from_slice(&[0u8; 200]);
    let apple = b"\x00\x05\x16\x07\x00\x02\x00\x00Mac OS X        ".to_vec();
    let z = zip(&[
        ("DICOMDIR", &dicomdir()),
        ("__MACOSX/", b""),
        ("__MACOSX/._IM1", &apple),
        ("viewer.exe", &exe),
        ("report.pdf", b"%PDF-1.7\n%junk"),
        ("DICOM/IM1", &im1),
    ]);
    let (r, rec, e) = run(&z, "cd.zip", &Limits::default());
    let stats = r.unwrap();
    assert_eq!(rec.paths(), vec!["cd.zip!/DICOM/IM1"]);
    let mut skipped = rec.skipped.clone();
    skipped.sort();
    assert_eq!(
        skipped,
        vec![
            "cd.zip!/DICOMDIR",
            "cd.zip!/__MACOSX/._IM1",
            "cd.zip!/report.pdf",
            "cd.zip!/viewer.exe"
        ]
    );
    assert!(rec.failed.is_empty());
    assert_eq!(stats.entries_seen, 5);
    assert_tmp_empty(&e.tmp);
}

#[test]
fn too_deep_fails_that_archive() {
    let limits = Limits {
        max_depth: 2,
        ..Limits::default()
    };
    // outer (0) > z1 (1) > z2 (2) > z3 (3: too deep)
    let z3 = zip(&[("IM3", &dicom("3"))]);
    let z2 = zip(&[("z3.zip", &z3), ("IM2", &dicom("2"))]);
    let z1 = zip(&[("z2.zip", &z2)]);
    let outer = zip(&[("z1.zip", &z1), ("IM0", &dicom("0"))]);
    let (r, rec, e) = run(&outer, "o.zip", &limits);
    r.unwrap();
    assert_eq!(
        rec.failed,
        vec![(
            "o.zip!/z1.zip!/z2.zip!/z3.zip".to_string(),
            "too deeply nested (limit 2)".to_string()
        )]
    );
    assert_eq!(
        rec.paths(),
        vec!["o.zip!/IM0", "o.zip!/z1.zip!/z2.zip!/IM2"]
    );
    assert_tmp_empty(&e.tmp);
}

#[test]
fn expanded_limit_aborts() {
    let mut bomb = dicom("BOMB");
    bomb.resize(10 * 1024 * 1024, 0);
    let z = zip(&[("IM1", &dicom("ok")), ("bomb", &bomb)]);
    assert!(z.len() < 100 * 1024);
    let limits = Limits {
        max_expanded_bytes: 1024 * 1024,
        ..Limits::default()
    };
    let (r, rec, e) = run(&z, "b.zip", &limits);
    match r {
        Err(WalkError::Aborted(reason)) => {
            assert_eq!(reason, "expanded size limit reached (1048576 bytes)")
        }
        other => panic!("expected abort, got {other:?}"),
    }
    assert_eq!(rec.paths(), vec!["b.zip!/IM1"]);
    assert_tmp_empty(&e.tmp);

    // nested bomb: abort while a spooled inner zip is being walked
    let inner = zip(&[("bomb", &bomb)]);
    let outer = zip(&[("in.zip", &inner)]);
    let (r, _rec, e) = run(&outer, "o.zip", &limits);
    assert!(matches!(r, Err(WalkError::Aborted(_))), "{r:?}");
    assert_tmp_empty(&e.tmp);
}

#[test]
fn entry_limit_aborts() {
    let limits = Limits {
        max_entries: 3,
        ..Limits::default()
    };
    let d = dicom("x");
    let z = zip(&[
        ("a", &d),
        ("dir/", b""),
        ("b", &d),
        ("c", &d),
        ("d", &d),
        ("e", &d),
    ]);
    // a ZIP that declares more entries than are left is refused before its
    // central directory is parsed
    let (r, rec, e) = run(&z, "z.zip", &limits);
    match r {
        Err(WalkError::Aborted(reason)) => assert_eq!(reason, "too many entries (limit 3)"),
        other => panic!("expected abort, got {other:?}"),
    }
    assert!(rec.leaves.is_empty());
    assert_tmp_empty(&e.tmp);

    // a stream (tar) aborts on the entry that crosses the limit
    let t = tar(&[
        TarItem::File("a", &d),
        TarItem::Dir("dir"),
        TarItem::File("b", &d),
        TarItem::File("c", &d),
        TarItem::File("d", &d),
    ]);
    let (r, rec, e) = run(&t, "t.tar", &limits);
    match r {
        Err(WalkError::Aborted(reason)) => assert_eq!(reason, "too many entries (limit 3)"),
        other => panic!("expected abort, got {other:?}"),
    }
    assert_eq!(rec.leaves.len(), 3);
    assert_tmp_empty(&e.tmp);

    // the budget is shared across nested archives
    let inner = zip(&[("x", &d), ("y", &d)]);
    let outer = zip(&[("i1.zip", &inner), ("i2.zip", &inner)]);
    let limits5 = Limits {
        max_entries: 5,
        ..Limits::default()
    };
    let (r, rec, e) = run(&outer, "o.zip", &limits5);
    assert!(matches!(r, Err(WalkError::Aborted(_))), "{r:?}");
    assert_eq!(rec.leaves.len(), 2);
    assert_tmp_empty(&e.tmp);

    // exactly at the limit is fine
    let z = zip(&[("a", &d), ("b", &d), ("c", &d)]);
    let (r, _rec, _e) = run(&z, "z.zip", &limits);
    assert_eq!(r.unwrap().entries_seen, 3);
}

#[test]
fn truncated_zip_fails_alone() {
    let inner = zip(&[("IM1", &dicom("1")), ("IM2", &dicom("2"))]);
    let cut = &inner[..inner.len() / 2];
    let t = tar(&[TarItem::File("x/IM9", &dicom("9"))]);
    let tcut = gz(&t)[..20].to_vec();
    let outer = zip(&[
        ("before", &dicom("b")),
        ("broken.zip", cut),
        ("broken.tgz", &tcut),
        ("after", &dicom("a")),
    ]);
    let (r, rec, e) = run(&outer, "o.zip", &Limits::default());
    r.unwrap();
    assert_eq!(rec.paths(), vec!["o.zip!/after", "o.zip!/before"]);
    let mut failed: Vec<&str> = rec.failed.iter().map(|(p, _)| p.as_str()).collect();
    failed.sort();
    assert_eq!(failed, vec!["o.zip!/broken.tgz", "o.zip!/broken.zip"]);
    for (_, reason) in &rec.failed {
        assert!(reason.starts_with("cannot read archive: "), "{reason}");
    }
    assert_tmp_empty(&e.tmp);

    // a truncated top-level zip fails that archive; the walk itself is Ok
    let (r, rec, _e) = run(cut, "cut.zip", &Limits::default());
    r.unwrap();
    assert_eq!(rec.failed.len(), 1);
    assert_eq!(rec.failed[0].0, "cut.zip");
}

#[test]
fn encrypted_zip_fails_alone() {
    let enc = mark_encrypted(zip(&[("IM1", &dicom("1"))]));
    let outer = zip(&[("enc.zip", &enc), ("IM2", &dicom("2"))]);
    let (r, rec, e) = run(&outer, "o.zip", &Limits::default());
    r.unwrap();
    assert_eq!(
        rec.failed,
        vec![(
            "o.zip!/enc.zip".to_string(),
            "encrypted archive".to_string()
        )]
    );
    assert_eq!(rec.paths(), vec!["o.zip!/IM2"]);
    assert_tmp_empty(&e.tmp);
}

#[test]
fn broken_seven_zip_fails_alone() {
    let seven = b"7z\xBC\xAF\x27\x1C\x00\x04rest".to_vec();
    let z = zip(&[("a.7z", &seven), ("IM", &dicom("1"))]);
    let (r, rec, e) = run(&z, "o.zip", &Limits::default());
    r.unwrap();
    assert_eq!(
        rec.failed,
        vec![(
            "o.zip!/a.7z".to_string(),
            "cannot read archive: truncated".to_string()
        )]
    );
    assert_eq!(rec.paths(), vec!["o.zip!/IM"]);
    assert_tmp_empty(&e.tmp);
}

#[test]
fn hostile_names_are_only_reported() {
    let z = zip(&[("../../etc/evil", &dicom("e")), ("/abs/IM", &dicom("f"))]);
    let (r, rec, e) = run(&z, "o.zip", &Limits::default());
    r.unwrap();
    assert_eq!(rec.paths(), vec!["o.zip!/../../etc/evil", "o.zip!//abs/IM"]);
    assert_tmp_empty(&e.tmp);
    assert!(!e.tmp.parent().unwrap().join("etc").exists());
}

#[test]
fn sink_error_is_io() {
    struct Failing;
    impl Sink for Failing {
        fn leaf(&mut self, _leaf: Leaf<'_>) -> io::Result<()> {
            Err(io::Error::other("disk on fire"))
        }
        fn event(&mut self, _ev: Event<'_>) {}
    }
    let z = zip(&[("IM", &dicom("1"))]);
    let e = env(&z);
    let (_, r) = walk_file(&e.input, "o.zip", &e.tmp, &Limits::default(), &mut Failing);
    assert!(matches!(r, Err(WalkError::Io(_))), "{r:?}");
    assert_tmp_empty(&e.tmp);
}

#[test]
fn tmp_dir_empty_after_walk() {
    let inner = zip(&[("IM", &dicom("1"))]);
    let tgz = gz(&tar(&[TarItem::File("in.zip", &inner)]));
    let outer = zip(&[("a.tgz", &tgz), ("b.zip", &inner)]);
    let (r, rec, e) = run(&outer, "o.zip", &Limits::default());
    r.unwrap();
    assert_eq!(
        rec.paths(),
        vec!["o.zip!/a.tgz!/in.zip!/IM", "o.zip!/b.zip!/IM"]
    );
    assert_tmp_empty(&e.tmp);

    // and after an abort in the middle of a nested walk
    let limits = Limits {
        max_entries: 2,
        ..Limits::default()
    };
    let (r, _rec, e) = run(&outer, "o.zip", &limits);
    assert!(matches!(r, Err(WalkError::Aborted(_))));
    assert_tmp_empty(&e.tmp);
}

#[test]
fn disk_guard_aborts() {
    // Free space is checked every 64 MiB counted; no volume has u64::MAX free.
    let limits = Limits {
        min_free_bytes: u64::MAX,
        ..Limits::default()
    };
    let mut big = dicom("BIG");
    big.resize(65 * 1024 * 1024, 0);
    let t = tar(&[TarItem::File("big", &big), TarItem::File("IM", &dicom("x"))]);
    let (r, rec, e) = run(&t, "t.tar", &limits);
    match r {
        Err(WalkError::Aborted(reason)) => assert_eq!(reason, "disk almost full"),
        other => panic!("expected abort, got {other:?}"),
    }
    assert!(rec.leaves.is_empty());
    assert_tmp_empty(&e.tmp);
}

#[test]
fn tar_oversized_metadata_header_fails_container() {
    let mut h = tar::Header::new_gnu();
    h.set_path("././@LongLink").unwrap();
    h.set_entry_type(tar::EntryType::GNULongName);
    h.set_size(20 * 1024 * 1024 * 1024);
    h.set_mode(0o644);
    h.set_cksum();
    let tgz = gz(&tar_raw_header(&h, &[0u8; 4096]));
    let z = zip(&[("evil.tgz", &tgz), ("IM", &dicom("1"))]);
    let (r, rec, e) = run(&z, "o.zip", &Limits::default());
    r.unwrap();
    assert_eq!(
        rec.failed,
        vec![(
            "o.zip!/evil.tgz".to_string(),
            "cannot read archive: oversized tar metadata header".to_string()
        )]
    );
    assert_eq!(rec.paths(), vec!["o.zip!/IM"]);
    assert_tmp_empty(&e.tmp);
}

#[test]
fn tar_long_names_and_pax_paths() {
    let long = format!("{}/IM1", "d".repeat(150));
    let a = dicom("A");
    let b = dicom("B");
    let t = tar(&[
        TarItem::File(&long, &a),
        TarItem::PaxFile("pax/name/IM2", &b),
    ]);
    let (r, rec, _e) = run(&gz(&t), "x.tgz", &Limits::default());
    r.unwrap();
    assert_eq!(rec.leaf(&format!("x.tgz!/{long}")).sha, sha(&a));
    assert_eq!(rec.leaf("x.tgz!/pax/name/IM2").sha, sha(&b));
    assert_eq!(rec.leaves.len(), 2);
}

#[test]
fn tar_sparse_entry_is_skipped() {
    let mut h = tar::Header::new_gnu();
    h.set_path("sparse").unwrap();
    h.set_entry_type(tar::EntryType::GNUSparse);
    h.set_size(0);
    h.as_gnu_mut().unwrap().set_real_size(1 << 40);
    h.set_mode(0o644);
    h.set_cksum();
    let (r, rec, _e) = run(&tar_raw_header(&h, &[]), "s.tar", &Limits::default());
    assert!(rec.failed.is_empty(), "{:?} {:?}", rec.failed, rec.skipped);
    assert_eq!(r.unwrap().entries_seen, 1);
    assert_eq!(rec.skipped, vec!["s.tar!/sparse"]);
}

#[test]
fn zip_central_directory_too_large_fails_container() {
    let big = set_cd_size(zip(&[("IM", &dicom("1"))]), 300 * 1024 * 1024);
    let outer = zip(&[("big.zip", &big), ("IM2", &dicom("2"))]);
    let (r, rec, e) = run(&outer, "o.zip", &Limits::default());
    r.unwrap();
    assert_eq!(
        rec.failed,
        vec![(
            "o.zip!/big.zip".to_string(),
            "cannot read archive: central directory too large".to_string()
        )]
    );
    assert_eq!(rec.paths(), vec!["o.zip!/IM2"]);
    assert_tmp_empty(&e.tmp);
}

#[test]
fn zip_bad_entry_fails_alone() {
    let z = corrupt(
        zip_stored(&[
            ("IM1", &dicom("ONE")),
            ("IM2", &dicom("CORRUPTED")),
            ("IM3", &dicom("THREE")),
        ]),
        b"CORRUPTED",
    );
    let (r, rec, e) = run(&z, "o.zip", &Limits::default());
    let stats = r.unwrap();
    assert_eq!(rec.paths(), vec!["o.zip!/IM1", "o.zip!/IM3"]);
    assert_eq!(rec.entry_failed.len(), 1, "{:?}", rec.entry_failed);
    assert_eq!(rec.entry_failed[0].0, "o.zip!/IM2");
    assert!(rec.entry_failed[0].1.starts_with("cannot read archive: "));
    assert!(rec.failed.is_empty());
    assert_eq!(stats.entries_seen, 3);
    assert_tmp_empty(&e.tmp);

    // one encrypted entry among plain ones fails only itself
    let z = mark_entry_encrypted(zip(&[("A", &dicom("a")), ("B", &dicom("b"))]), "B");
    let (r, rec, _e) = run(&z, "p.zip", &Limits::default());
    r.unwrap();
    assert_eq!(rec.paths(), vec!["p.zip!/A"]);
    assert_eq!(
        rec.entry_failed,
        vec![("p.zip!/B".to_string(), "encrypted archive".to_string())]
    );
    assert!(rec.failed.is_empty());
}

#[test]
fn gzip_of_zip_is_walked() {
    let d = dicom("1");
    let zgz = gz(&zip(&[("IM1", &d)]));
    let (r, rec, e) = run(&zgz, "x.zip.gz", &Limits::default());
    r.unwrap();
    assert_eq!(rec.paths(), vec!["x.zip.gz!/IM1"]);
    assert_eq!(rec.leaf("x.zip.gz!/IM1").sha, sha(&d));
    assert_tmp_empty(&e.tmp);

    // the decompressed layer counts as a nesting level
    let limits = Limits {
        max_depth: 0,
        ..Limits::default()
    };
    let (r, rec, e) = run(&zgz, "x.zip.gz", &limits);
    r.unwrap();
    assert_eq!(
        rec.failed,
        vec![(
            "x.zip.gz".to_string(),
            "too deeply nested (limit 0)".to_string()
        )]
    );
    assert_tmp_empty(&e.tmp);
}

#[test]
fn tar_extended_sparse_fails_container() {
    let mut h = tar::Header::new_gnu();
    h.set_path("sparse").unwrap();
    h.set_entry_type(tar::EntryType::GNUSparse);
    h.set_size(0);
    h.set_mode(0o644);
    let g = h.as_gnu_mut().unwrap();
    g.set_real_size(1 << 40);
    g.isextended[0] = 1;
    h.set_cksum();
    let (r, rec, _e) = run(&tar_raw_header(&h, &[]), "s.tar", &Limits::default());
    r.unwrap();
    assert_eq!(
        rec.failed,
        vec![(
            "s.tar".to_string(),
            "cannot read archive: unsupported extended sparse tar entry".to_string()
        )]
    );
}

#[test]
fn zip64_with_many_entries_is_walked() {
    const N: usize = 70_000;
    let names: Vec<String> = (0..N).map(|i| format!("f{i}")).collect();
    let mut entries: Vec<(&str, &[u8])> = names.iter().map(|n| (n.as_str(), &b"x"[..])).collect();
    let d = dicom("last");
    entries.push(("IM", &d));
    let z = zip_stored(&entries);
    let eocd = z.windows(4).rposition(|w| w == b"PK\x05\x06").unwrap();
    assert_eq!(
        &z[eocd + 10..eocd + 12],
        &[0xFF, 0xFF],
        "fixture must be ZIP64"
    );
    let (r, rec, e) = run(&z, "big.zip", &Limits::default());
    let stats = r.unwrap();
    assert!(rec.failed.is_empty(), "{:?}", rec.failed);
    assert_eq!(rec.skipped.len(), N);
    assert_eq!(rec.paths(), vec!["big.zip!/IM"]);
    assert_eq!(stats.entries_seen, N as u64 + 1);
    assert_tmp_empty(&e.tmp);
}

#[test]
fn unread_leaf_is_skipped_in_tar_gz() {
    let mut a = dicom("A");
    a.resize(200_000, 7);
    let b = dicom("B");
    let tgz = gz(&tar(&[TarItem::File("IM1", &a), TarItem::File("IM2", &b)]));
    let rec = Recorder {
        unread: vec!["t.tgz!/IM1".to_string()],
        ..Recorder::default()
    };
    let (_, r, rec, e) = run_rec(&tgz, "t.tgz", &Limits::default(), rec);
    r.unwrap();
    assert_eq!(rec.paths(), vec!["t.tgz!/IM2"]);
    assert_eq!(rec.leaves[0].sha, sha(&b));
    assert_tmp_empty(&e.tmp);
}

#[test]
fn stats_survive_abort_and_progress_is_reported() {
    let d = dicom("x");
    let z = zip(&[("a", &d), ("b", &d), ("c", &d)]);
    let (stats, r, rec, _e) = run_rec(&z, "z.zip", &Limits::default(), Recorder::default());
    r.unwrap();
    assert_eq!(stats.entries_seen, 3);
    let entries: Vec<u64> = rec.progress.iter().map(|p| p.0).collect();
    assert_eq!(entries, vec![1, 2, 3]);
    // bytes lag by the entry being counted
    assert_eq!(rec.progress[2].1, 2 * d.len() as u64);

    let mut bomb = dicom("BOMB");
    bomb.resize(4 * 1024 * 1024, 0);
    let z = zip(&[("IM1", &d), ("bomb", &bomb)]);
    let limits = Limits {
        max_expanded_bytes: 1024 * 1024,
        ..Limits::default()
    };
    let (stats, r, _rec, _e) = run_rec(&z, "b.zip", &limits, Recorder::default());
    assert!(matches!(r, Err(WalkError::Aborted(_))), "{r:?}");
    assert_eq!(stats.entries_seen, 2);
    assert!(stats.bytes_expanded > 1024 * 1024, "{stats:?}");
}

#[test]
fn continued_walk_shares_the_budget() {
    let d = dicom("x");
    let z = zip(&[("a", &d), ("b", &d)]);
    let e = env(&z);
    let limits = Limits {
        max_entries: 3,
        ..Limits::default()
    };
    let spent = WalkStats {
        entries_seen: 2,
        bytes_expanded: 10,
    };
    let mut rec = Recorder::default();
    let (stats, r) = walk_file_continue(&e.input, "z.zip", &e.tmp, &limits, spent, &mut rec);
    match r {
        Err(WalkError::Aborted(reason)) => assert_eq!(reason, "too many entries (limit 3)"),
        other => panic!("expected abort, got {other:?}"),
    }
    assert_eq!(stats, spent);

    let limits = Limits {
        max_entries: 4,
        ..Limits::default()
    };
    let mut rec = Recorder::default();
    let (stats, r) = walk_file_continue(&e.input, "z.zip", &e.tmp, &limits, spent, &mut rec);
    r.unwrap();
    assert_eq!(stats.entries_seen, 4);
    assert_eq!(stats.bytes_expanded, 10 + 2 * d.len() as u64);
    assert_eq!(rec.progress.first().map(|p| p.0), Some(3));
}
