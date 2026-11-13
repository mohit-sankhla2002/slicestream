//! `slicestream-server import` run as the real binary.

use std::path::Path;
use std::process::{Command, Output};

mod support;
use support::*;

/// Run `import <args>` against a fresh store under `dir`.
fn import(dir: &Path, args: &[&str]) -> Output {
    let config = dir.join("config.toml");
    std::fs::write(
        &config,
        format!(
            "[storage]\nroot = {:?}\n",
            dir.join("store").display().to_string()
        ),
    )
    .unwrap();
    Command::new(env!("CARGO_BIN_EXE_slicestream-server"))
        .arg("--config")
        .arg(&config)
        .arg("import")
        .args(args)
        .output()
        .unwrap()
}

fn stdout(o: &Output) -> String {
    String::from_utf8_lossy(&o.stdout).into_owned()
}

#[test]
fn import_zip_file() {
    let dir = tempfile::tempdir().unwrap();
    let (a, b) = (dicom(1, 1, 1, 1), dicom(1, 1, 1, 2));
    let z = zip(&[("a.dcm", &a), ("b.dcm", &b)]);
    let path = dir.path().join("scan.zip");
    std::fs::write(&path, z).unwrap();

    let out = import(dir.path(), &[path.to_str().unwrap()]);
    let text = stdout(&out);
    assert!(out.status.success(), "{text}\n{:?}", out.stderr);
    assert!(
        text.contains("P1 TEST^PATIENT1: 1 studies, 2 images"),
        "{text}"
    );
}

#[test]
fn import_folder_with_nested_zip_and_loose_nifti() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("in");
    std::fs::create_dir_all(&data).unwrap();
    let inner = zip(&[("1.dcm", &dicom(1, 1, 1, 1)), ("2.dcm", &dicom(1, 1, 1, 2))]);
    std::fs::write(data.join("outer.zip"), zip(&[("inner.zip", &inner)])).unwrap();
    std::fs::write(data.join("brain.nii"), nifti(1)).unwrap();
    std::fs::write(data.join("notes.txt"), b"hello").unwrap();

    let out = import(dir.path(), &[data.to_str().unwrap()]);
    let text = stdout(&out);
    assert!(out.status.success(), "{text}\n{:?}", out.stderr);
    assert!(
        text.contains("P1 TEST^PATIENT1: 1 studies, 2 images"),
        "{text}"
    );
    assert!(
        text.contains(
            "stored 2 · duplicate 0 · nifti 1 (linked 1, unassigned 0) · skipped 1 · failed 0"
        ),
        "{text}"
    );
}

#[test]
fn import_prints_summary() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("in");
    std::fs::create_dir_all(&data).unwrap();
    std::fs::write(data.join("a.dcm"), dicom(1, 1, 1, 1)).unwrap();
    std::fs::write(data.join("b.zip"), zip(&[("x.dcm", &dicom(2, 1, 1, 1))])).unwrap();
    std::fs::write(data.join("bad.zip"), b"PK\x03\x04 truncated").unwrap();

    let out = import(dir.path(), &[data.to_str().unwrap()]);
    let text = stdout(&out);
    assert!(
        text.contains(
            "stored 2 · duplicate 0 · nifti 0 (linked 0, unassigned 0) · skipped 0 · failed 1"
        ),
        "{text}"
    );
    assert!(!out.status.success(), "a failed entry exits non-zero");

    // Again: the DICOM are duplicates now.
    let out = import(dir.path(), &[data.join("a.dcm").to_str().unwrap()]);
    let text = stdout(&out);
    assert!(out.status.success(), "{text}");
    assert!(text.contains("stored 0 · duplicate 1 ·"), "{text}");
}
