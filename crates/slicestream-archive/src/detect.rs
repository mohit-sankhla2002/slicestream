use crate::nifti;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Zip,
    SevenZ,
    Rar,
    Gzip,
    Tar,
    Dicom,
    Nifti,
    Other,
}

pub const SNIFF_LEN: usize = 1024;

pub fn detect(head: &[u8]) -> Kind {
    if head.starts_with(b"PK\x03\x04") || head.starts_with(b"PK\x05\x06") {
        Kind::Zip
    } else if head.starts_with(b"7z\xBC\xAF\x27\x1C") {
        Kind::SevenZ
    } else if head.starts_with(b"Rar!\x1A\x07\x00") || head.starts_with(b"Rar!\x1A\x07\x01\x00") {
        Kind::Rar
    } else if head.starts_with(b"\x1F\x8B") {
        Kind::Gzip
    } else if head.get(257..262) == Some(b"ustar".as_slice()) {
        Kind::Tar
    } else if head.get(128..132) == Some(b"DICM".as_slice()) {
        Kind::Dicom
    } else if nifti::sniff(head).is_some() {
        Kind::Nifti
    } else {
        Kind::Other
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(off: usize, bytes: &[u8], len: usize) -> Vec<u8> {
        let mut v = vec![0u8; len];
        v[off..off + bytes.len()].copy_from_slice(bytes);
        v
    }

    #[test]
    fn detect_each_magic() {
        assert_eq!(detect(b"PK\x03\x04rest"), Kind::Zip);
        assert_eq!(detect(b"PK\x05\x06\0\0\0\0"), Kind::Zip);
        assert_eq!(detect(b"7z\xBC\xAF\x27\x1C\0\x04"), Kind::SevenZ);
        assert_eq!(detect(b"Rar!\x1A\x07\x00xx"), Kind::Rar);
        assert_eq!(detect(b"Rar!\x1A\x07\x01\x00xx"), Kind::Rar);
        assert_eq!(detect(b"\x1F\x8B\x08\0"), Kind::Gzip);
        assert_eq!(detect(&at(257, b"ustar", 512)), Kind::Tar);
        assert_eq!(detect(&at(128, b"DICM", 256)), Kind::Dicom);
        assert_eq!(
            detect(&nifti::tests_support::nifti1_head(false)),
            Kind::Nifti
        );
        assert_eq!(
            detect(&nifti::tests_support::nifti1_head(true)),
            Kind::Nifti
        );
        assert_eq!(detect(&nifti::tests_support::nifti2_head()), Kind::Nifti);
    }

    #[test]
    fn detect_short_and_junk_is_other() {
        assert_eq!(detect(b""), Kind::Other);
        assert_eq!(detect(b"abc"), Kind::Other);
        assert_eq!(detect(b"hello world, plain text"), Kind::Other);
        assert_eq!(detect(b"%PDF-1.7\n"), Kind::Other);
    }

    #[test]
    fn detect_nifti_requires_both_size_and_magic() {
        // size only
        let mut v = vec![0u8; 400];
        v[0..4].copy_from_slice(&348i32.to_le_bytes());
        assert_eq!(detect(&v), Kind::Other);
        // magic only
        let v = at(344, b"n+1\0", 400);
        assert_eq!(detect(&v), Kind::Other);
        // ni1 magic accepted
        let mut v = at(344, b"ni1\0", 400);
        v[0..4].copy_from_slice(&348i32.to_le_bytes());
        assert_eq!(detect(&v), Kind::Nifti);
        // nifti-2 size without magic
        let mut v = vec![0u8; 600];
        v[0..4].copy_from_slice(&540i32.to_le_bytes());
        assert_eq!(detect(&v), Kind::Other);
    }
}
