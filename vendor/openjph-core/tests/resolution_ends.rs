//! `Codestream::resolution_ends` reports where each resolution's packets end.

use openjph_core::codestream::Codestream;
use openjph_core::file::MemOutfile;
use openjph_core::types::{Point, Size};

const SOT: u16 = 0xFF90;
const EOC: u16 = 0xFFD9;

fn configure(cs: &mut Codestream, w: u32, h: u32, levels: u32, tlm: bool) {
    let siz = cs.access_siz_mut();
    siz.set_image_extent(Point::new(w, h));
    siz.set_num_components(1);
    siz.set_comp_info(0, Point::new(1, 1), 12, false);
    siz.set_tile_size(Size::new(w, h));
    let cod = cs.access_cod_mut();
    cod.set_num_decomposition(levels);
    cod.set_block_dims(64, 64);
    cod.set_reversible(true);
    cod.set_color_transform(false);
    let _ = cod.set_progression_order("RPCL");
    cs.set_planar(0);
    cs.request_tlm_marker(tlm);
}

/// Encode a 12-bit gradient with a little texture; return the codestream and
/// what `resolution_ends` said after `flush`.
fn encode_gradient(w: u32, h: u32, levels: u32, tlm: bool) -> (Vec<u8>, Option<Vec<usize>>) {
    let mut cs = Codestream::new();
    configure(&mut cs, w, h, levels, tlm);
    let mut out = MemOutfile::new();
    cs.write_headers(&mut out, &[]).unwrap();
    for y in 0..h {
        let line: Vec<i32> = (0..w)
            .map(|x| (((x * 3 + y * 5) % 4096) as i32 ^ ((x * y) % 7) as i32) & 0xFFF)
            .collect();
        cs.exchange(&line, 0).unwrap();
    }
    cs.flush(&mut out).unwrap();
    let ends = cs.resolution_ends();
    (out.get_data().to_vec(), ends)
}

/// Offset of the SOT marker: walk the main header's marker segments.
fn main_header_len(bytes: &[u8]) -> usize {
    let mut pos = 2; // SOC
    loop {
        let marker = u16::from_be_bytes([bytes[pos], bytes[pos + 1]]);
        if marker == SOT {
            return pos;
        }
        let len = u16::from_be_bytes([bytes[pos + 2], bytes[pos + 3]]) as usize;
        pos += 2 + len;
    }
}

#[test]
fn ends_cover_every_resolution() {
    for tlm in [true, false] {
        let (bytes, ends) = encode_gradient(512, 512, 3, tlm);
        let ends = ends.expect("ends after flush");
        assert_eq!(ends.len(), 4);
        assert!(ends.windows(2).all(|w| w[0] < w[1]), "{ends:?}");
        assert_eq!(*ends.last().unwrap(), bytes.len());
        assert_eq!(&bytes[bytes.len() - 2..], &EOC.to_be_bytes());
        // the first resolution ends after the main header, SOT and SOD
        assert!(ends[0] > main_header_len(&bytes) + 12 + 2, "tlm={tlm}");
    }
}

#[test]
fn ends_none_before_flush() {
    assert_eq!(Codestream::new().resolution_ends(), None);
}

#[test]
fn zero_levels_has_one_end() {
    let (bytes, ends) = encode_gradient(32, 32, 0, true);
    assert_eq!(ends, Some(vec![bytes.len()]));
}
