#[path = "../examples/make_rle_multiframe.rs"]
mod rle;

use dicom_dictionary_std::tags;
use dicom_object::OpenFileOptions;
use slicestream_core::FrameInfo;
use slicestream_core::frame::read_frame_bytes;

/// PackBits decoder (PS3.5 G.3.2), written apart from the encoder.
fn unpack_bits(src: &[u8], want: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(want);
    let mut i = 0;
    while out.len() < want {
        let n = src[i] as i8;
        i += 1;
        if n >= 0 {
            let len = n as usize + 1;
            out.extend_from_slice(&src[i..i + len]);
            i += len;
        } else if n != -128 {
            let len = 1 - n as isize;
            out.extend(std::iter::repeat_n(src[i], len as usize));
            i += 1;
        }
    }
    assert_eq!(out.len(), want, "a run crosses the end of the segment");
    out
}

/// Decode one 16-bit, one-sample RLE frame to little-endian pixels.
fn decode_frame(frame: &[u8], pixels: usize) -> Vec<u8> {
    let word = |k: usize| u32::from_le_bytes(frame[4 * k..4 * k + 4].try_into().unwrap());
    assert_eq!(word(0), 2, "one segment per byte of a 16-bit sample");
    assert_eq!(word(1), 64, "the first segment follows the 64-byte header");
    for k in 3..16 {
        assert_eq!(word(k), 0, "unused header offset {k}");
    }
    let (s1, s2) = (word(1) as usize, word(2) as usize);
    let high = unpack_bits(&frame[s1..s2], pixels);
    let low = unpack_bits(&frame[s2..], pixels);
    low.iter().zip(&high).flat_map(|(&l, &h)| [l, h]).collect()
}

#[test]
fn frames_decode_to_phantom_pixels() {
    let dir = tempfile::tempdir().unwrap();
    let (frames, size) = (5, 32_u16);
    let path = rle::write_rle_multiframe(dir.path(), frames, size).unwrap();

    let obj = OpenFileOptions::new().open_file(&path).unwrap();
    assert_eq!(obj.meta().transfer_syntax(), rle::RLE_LOSSLESS);
    let n = obj.element(tags::NUMBER_OF_FRAMES).unwrap().to_int::<u32>();
    assert_eq!(n.unwrap(), frames as u32);

    // The Basic Offset Table points at each frame's fragment item.
    let pixel = obj.element(tags::PIXEL_DATA).unwrap().value();
    let fragments = pixel.fragments().unwrap();
    let table = pixel.offset_table().unwrap();
    assert_eq!(fragments.len(), frames);
    let mut at = 0;
    for (k, f) in fragments.iter().enumerate() {
        assert_eq!(table[k], at, "offset of frame {k}");
        assert_eq!(f.len() % 2, 0, "fragment {k} has odd length");
        at += 8 + f.len() as u32;
    }

    let ts = obj.meta().transfer_syntax().to_string();
    let info = FrameInfo::from_dataset(&obj, &ts).unwrap();
    let pixels = size as usize * size as usize;
    for k in 0..frames {
        let frame = read_frame_bytes(&path, k as u32, &info).unwrap();
        let expected = rle::phantom::phantom_pixels(k, frames, size);
        assert_eq!(decode_frame(&frame, pixels), expected, "frame {k}");
    }
}

#[test]
fn pack_bits_round_trips_runs_and_literals() {
    let mut rows: Vec<Vec<u8>> = vec![
        vec![7],
        vec![1, 2],
        vec![5; 2],
        vec![5; 3],
        vec![9; 128],
        vec![9; 129],
        vec![9; 300],
        (0..=255).collect(),
        (0..300).map(|i| (i % 3) as u8).collect(),
    ];
    // Literals around a run, and a run at either end.
    rows.push([&[1, 2, 3][..], &[4; 10], &[5, 6]].concat());
    rows.push([&[4; 10][..], &[1, 2, 3]].concat());
    rows.push([&[1, 2, 3][..], &[4; 200]].concat());
    for row in rows {
        let mut out = Vec::new();
        rle::pack_bits(&row, &mut out);
        assert_eq!(unpack_bits(&out, row.len()), row, "row {row:?}");
        // Never worse than one header byte per 128 literal bytes.
        assert!(out.len() <= row.len() + row.len().div_ceil(128));
    }
}
