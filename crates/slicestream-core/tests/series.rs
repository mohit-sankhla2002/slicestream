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
