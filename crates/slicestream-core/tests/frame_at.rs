#[path = "../examples/make_phantom.rs"]
mod phantom;

use std::io::Write;

use slicestream_core::frame::{FrameLocation, read_frame_at, read_frame_bytes};
use slicestream_core::{Error, FrameInfo, Series, SliceRef};

fn phantom_file(size: u16) -> (tempfile::TempDir, std::path::PathBuf, FrameInfo) {
    let dir = tempfile::tempdir().unwrap();
    phantom::write_phantom(dir.path(), 1, size).unwrap();
    let s = &Series::scan_dir(dir.path()).unwrap()[0];
    let r = s.slice(0).unwrap();
    (dir, r.path.clone(), r.info.clone())
}

#[test]
fn read_frame_at_matches_parse_native() {
    let (_dir, path, info) = phantom_file(16);
    let expected = read_frame_bytes(&path, 0, &info).unwrap();
    let file_len = std::fs::metadata(&path).unwrap().len();
    let len = info.native_frame_len() as u64;
    // Pixel Data is the last element and has a defined length.
    let loc = FrameLocation::Bytes {
        offset: file_len - len,
        len,
    };
    assert_eq!(read_frame_at(&path, &loc).unwrap(), expected);
}

#[test]
fn read_frame_at_fragments_concatenates() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("f.bin");
    std::fs::write(&path, b"0123456789").unwrap();
    let loc = FrameLocation::Fragments(vec![(6, 3), (1, 2)]);
    assert_eq!(&read_frame_at(&path, &loc).unwrap()[..], b"67812");
}

#[test]
fn read_frame_at_bits_extracts() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("b.bin");
    std::fs::write(&path, [0xFF, 0b1010_1100, 0b0000_0111]).unwrap();
    let loc = FrameLocation::Bits {
        offset: 1,
        bit_start: 3,
        bit_len: 10,
    };
    assert_eq!(
        &read_frame_at(&path, &loc).unwrap()[..],
        &[0b1111_0101, 0b0000_0000]
    );
}

#[test]
fn read_frame_at_short_file_is_io_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("short.bin");
    std::fs::write(&path, b"abc").unwrap();
    let loc = FrameLocation::Bytes { offset: 1, len: 10 };
    assert!(matches!(read_frame_at(&path, &loc), Err(Error::Io { .. })));
}

#[test]
fn load_frame_uses_location() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("scratch.bin");
    let mut f = std::fs::File::create(&path).unwrap();
    f.write_all(b"xxxxHELLOyyyy").unwrap();
    drop(f);

    let (_d, _p, info) = phantom_file(16);
    let slice = SliceRef::new(path, 0, info, None, None)
        .with_location(FrameLocation::Bytes { offset: 4, len: 5 });
    let series = Series::from_slices("1.2".into(), None, None, vec![slice]);
    let frame = series.load_frame(0).unwrap();
    assert_eq!(&frame.data[..], b"HELLO");
}
