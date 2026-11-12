#[path = "../examples/make_phantom.rs"]
mod phantom;

use slicestream_core::{PrefetchScheduler, SchedulerConfig, Series, protocol};

#[test]
fn scans_sorts_and_loads_phantom() {
    let dir = tempfile::tempdir().unwrap();
    phantom::write_phantom(dir.path(), 10, 32).unwrap();
    std::fs::write(dir.path().join("README.txt"), "not dicom").unwrap();

    let series = Series::scan_dir(dir.path()).unwrap();
    assert_eq!(series.len(), 1);
    let s = &series[0];
    assert_eq!(s.len(), 10);
    assert_eq!(s.modality.as_deref(), Some("CT"));

    // Files were written in reverse; sorted stack must follow slice position.
    let first = s.slice(0).unwrap().path.file_name().unwrap().to_owned();
    assert_eq!(first, "IM00010.dcm");

    let frame = s.load_frame(3).unwrap();
    assert_eq!(frame.data.len(), 32 * 32 * 2);
    assert_eq!(frame.info.window_width, Some(400.0));
    assert!(!frame.info.is_encapsulated());

    let msg = protocol::encode_frame(&frame);
    let (header, payload) = protocol::decode_frame(&msg).unwrap();
    assert_eq!(header.index, 3);
    assert_eq!(payload.len(), frame.data.len());
}

#[test]
fn scheduler_drives_full_series() {
    let dir = tempfile::tempdir().unwrap();
    phantom::write_phantom(dir.path(), 8, 16).unwrap();
    let s = &Series::scan_dir(dir.path()).unwrap()[0];
    let mut sched = PrefetchScheduler::new(s.len(), SchedulerConfig::default());
    sched.update(4, 0.0);
    let mut n = 0;
    while let Some(i) = sched.pop_next() {
        s.load_frame(i).unwrap();
        n += 1;
    }
    assert_eq!(n, 8);
}

#[test]
fn from_slices_sorts_by_position_then_instance() {
    use slicestream_core::{FrameInfo, SliceRef};
    use std::path::PathBuf;

    let info = FrameInfo {
        rows: 1,
        columns: 1,
        bits_allocated: 16,
        bits_stored: 16,
        samples_per_pixel: 1,
        pixel_representation: 0,
        photometric_interpretation: "MONOCHROME2".into(),
        transfer_syntax: "1.2.840.10008.1.2.1".into(),
        rescale_slope: 1.0,
        rescale_intercept: 0.0,
        window_center: None,
        window_width: None,
    };
    let mk = |name: &str, pos: Option<f64>, inst: i64| {
        SliceRef::new(PathBuf::from(name), 0, info.clone(), pos, Some(inst))
    };
    let s = Series::from_slices(
        "1.2.3".into(),
        None,
        None,
        vec![
            mk("pos2", Some(2.0), 1),
            mk("none", None, 5),
            mk("pos1", Some(1.0), 9),
        ],
    );
    let names: Vec<_> = (0..s.len())
        .map(|i| s.slice(i).unwrap().path.to_str().unwrap().to_owned())
        .collect();
    assert_eq!(names, ["none", "pos1", "pos2"]);
}
