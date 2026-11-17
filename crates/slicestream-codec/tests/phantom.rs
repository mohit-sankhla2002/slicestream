#[allow(dead_code)]
#[path = "../../slicestream-core/examples/make_phantom.rs"]
mod phantom;

use slicestream_codec::encode_verified;
use slicestream_core::FrameInfo;

#[test]
fn phantom_series_round_trips() {
    let size = 256u16;
    let info = FrameInfo {
        rows: size,
        columns: size,
        bits_allocated: 16,
        bits_stored: 16,
        samples_per_pixel: 1,
        pixel_representation: 0,
        photometric_interpretation: "MONOCHROME2".into(),
        transfer_syntax: "1.2.840.10008.1.2.1".into(),
        rescale_slope: 1.0,
        rescale_intercept: -1024.0,
        window_center: None,
        window_width: None,
    };
    for z in 0..4 {
        let frame = phantom::phantom_pixels(z, 4, size);
        let e = encode_verified(&frame, &info).unwrap();
        assert_eq!(e.ends.len(), 3, "256 px -> 2 levels");
    }
}
