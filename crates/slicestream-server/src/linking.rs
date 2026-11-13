//! NIfTI → patient/study linking by path scope within one upload.
//!
//! Paths are upload-relative: nested archive members are joined with `!/`
//! (`all.zip!/a.zip!/dir/x.nii.gz`), folder imports use `/`. The scopes of a
//! path are its ancestor prefixes, nearest first: the prefix up to and
//! including each `/` (which also covers each `!/`), then the empty prefix
//! (the whole upload). Only the nearest [`MAX_SCOPE_DEPTH`] prefixes of a
//! path take part, so a deeply nested path costs a bounded amount of work.

use std::collections::HashMap;

/// The patients and studies seen under one scope.
#[derive(Clone, Copy)]
struct Scope {
    patient: i64,
    many_patients: bool,
    study: i64,
    many_studies: bool,
}

impl Scope {
    fn new(patient: i64, study: i64) -> Self {
        Scope {
            patient,
            many_patients: false,
            study,
            many_studies: false,
        }
    }

    fn add(&mut self, patient: i64, study: i64) {
        self.many_patients |= patient != self.patient;
        self.many_studies |= study != self.study;
    }

    fn merge(&mut self, other: &Scope) {
        self.add(other.patient, other.study);
        self.many_patients |= other.many_patients;
        self.many_studies |= other.many_studies;
    }
}

/// Ancestor prefixes considered per path (besides the whole upload).
const MAX_SCOPE_DEPTH: usize = 64;

/// Ancestor prefixes of `path`, nearest first (at most
/// [`MAX_SCOPE_DEPTH`]), ending with `""`.
fn scopes(path: &str) -> impl Iterator<Item = &str> {
    path.rmatch_indices('/')
        .take(MAX_SCOPE_DEPTH)
        .map(move |(i, _)| &path[..=i])
        .chain(std::iter::once(""))
}

/// The directory of `path`: its nearest scope.
fn dir(path: &str) -> &str {
    path.rfind('/').map_or("", |i| &path[..=i])
}

/// Link each NIfTI `(id, path)` using the stored/duplicate DICOM of the same
/// upload, `(path, patient_fk, study_fk)`.
///
/// The nearest scope that contains any DICOM decides: one patient → that
/// patient, plus the study if the scope holds exactly one; several patients
/// → unassigned. No DICOM in any scope → unassigned. Returns
/// `(nifti_id, patient_fk, study_fk)` in input order.
pub fn link(
    nifti: &[(i64, String)],
    dicom: &[(String, i64, i64)],
) -> Vec<(i64, Option<i64>, Option<i64>)> {
    if nifti.is_empty() {
        return Vec::new();
    }
    // Many DICOM share a directory: aggregate per directory first, so each
    // distinct directory's ancestors are visited once.
    let mut by_dir: HashMap<&str, Scope> = HashMap::new();
    for (path, patient, study) in dicom {
        by_dir
            .entry(dir(path))
            .and_modify(|s| s.add(*patient, *study))
            .or_insert_with(|| Scope::new(*patient, *study));
    }
    let mut by_scope: HashMap<&str, Scope> = HashMap::new();
    for (d, scope) in &by_dir {
        for prefix in scopes(d) {
            by_scope
                .entry(prefix)
                .and_modify(|s| s.merge(scope))
                .or_insert(*scope);
        }
    }
    nifti
        .iter()
        .map(
            |(id, path)| match scopes(path).find_map(|p| by_scope.get(p)) {
                Some(s) if !s.many_patients => {
                    (*id, Some(s.patient), (!s.many_studies).then_some(s.study))
                }
                _ => (*id, None, None),
            },
        )
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(path: &str, patient: i64, study: i64) -> (String, i64, i64) {
        (path.to_string(), patient, study)
    }

    fn n(id: i64, path: &str) -> (i64, String) {
        (id, path.to_string())
    }

    #[test]
    fn link_same_dir() {
        let dicom = [d("u.zip!/p1/IM1", 1, 10), d("u.zip!/p1/IM2", 1, 10)];
        assert_eq!(
            link(&[n(7, "u.zip!/p1/seg.nii.gz")], &dicom),
            vec![(7, Some(1), Some(10))]
        );
    }

    #[test]
    fn link_nested_per_patient() {
        let dicom = [
            d("all.zip!/a.zip!/ct/IM1", 1, 10),
            d("all.zip!/a.zip!/ct/IM2", 1, 10),
            d("all.zip!/b.zip!/ct/IM1", 2, 20),
        ];
        let nifti = [
            n(1, "all.zip!/a.zip!/x.nii.gz"),
            n(2, "all.zip!/b.zip!/seg/y.nii"),
        ];
        assert_eq!(
            link(&nifti, &dicom),
            vec![(1, Some(1), Some(10)), (2, Some(2), Some(20))]
        );
    }

    #[test]
    fn link_two_patients_in_scope_unassigned() {
        let dicom = [d("u.zip!/IM1", 1, 10), d("u.zip!/IM2", 2, 20)];
        // The nearest scope with DICOM has two patients: stop there, even
        // though an outer scope would not help either.
        assert_eq!(link(&[n(3, "u.zip!/x.nii")], &dicom), vec![(3, None, None)]);
        // A nearer scope with one patient wins over a mixed outer scope.
        let dicom = [d("u.zip!/a/IM1", 1, 10), d("u.zip!/b/IM1", 2, 20)];
        assert_eq!(
            link(&[n(4, "u.zip!/a/x.nii")], &dicom),
            vec![(4, Some(1), Some(10))]
        );
    }

    #[test]
    fn link_no_dicom_unassigned() {
        assert_eq!(link(&[n(5, "x.nii.gz")], &[]), vec![(5, None, None)]);
        assert_eq!(link(&[n(5, "u.zip!/x.nii.gz")], &[]), vec![(5, None, None)]);
    }

    #[test]
    fn link_study_only_when_unique() {
        let dicom = [d("u.zip!/s1/IM1", 1, 10), d("u.zip!/s2/IM1", 1, 11)];
        assert_eq!(
            link(&[n(6, "u.zip!/x.nii")], &dicom),
            vec![(6, Some(1), None)]
        );
    }

    #[test]
    fn link_folder_scopes() {
        let dicom = [d("sub/IM1", 1, 10), d("other/IM1", 2, 20)];
        assert_eq!(
            link(&[n(8, "sub/x.nii"), n(9, "top.nii")], &dicom),
            vec![(8, Some(1), Some(10)), (9, None, None)]
        );
        // The import root is the outermost scope.
        assert_eq!(
            link(&[n(10, "x.nii")], &[d("IM1", 3, 30)]),
            vec![(10, Some(3), Some(30))]
        );
    }

    #[test]
    fn link_no_nifti_is_empty() {
        let dicom = [d("u.zip!/IM1", 1, 10)];
        assert!(link(&[], &dicom).is_empty());
    }

    #[test]
    fn link_merges_mixed_directories() {
        // One directory already mixed: its parent must be mixed too.
        let dicom = [
            d("u.zip!/a/b/IM1", 1, 10),
            d("u.zip!/a/b/IM2", 2, 20),
            d("u.zip!/a/c/IM1", 1, 10),
        ];
        assert_eq!(
            link(&[n(1, "u.zip!/a/x.nii")], &dicom),
            vec![(1, None, None)]
        );
        assert_eq!(
            link(&[n(2, "u.zip!/a/c/x.nii")], &dicom),
            vec![(2, Some(1), Some(10))]
        );
    }

    #[test]
    fn link_huge_deep_paths_bounded() {
        // 1 MiB of `a/` per path: ~512k ancestors each if unbounded.
        let deep = "a/".repeat(512 * 1024);
        let dicom: Vec<_> = (0..8).map(|i| d(&format!("{deep}IM{i}"), 1, 10)).collect();
        let start = std::time::Instant::now();
        let got = link(&[n(1, &format!("{deep}seg.nii")), n(2, "top.nii")], &dicom);
        assert!(start.elapsed() < std::time::Duration::from_secs(5));
        // Same directory links; the whole upload holds one patient too.
        assert_eq!(got, vec![(1, Some(1), Some(10)), (2, Some(1), Some(10))]);
        assert_eq!(scopes(&deep).count(), MAX_SCOPE_DEPTH + 1);
    }

    #[test]
    fn scopes_nearest_first() {
        assert_eq!(
            scopes("a.zip!/b/c.nii").collect::<Vec<_>>(),
            vec!["a.zip!/b/", "a.zip!/", ""]
        );
        assert_eq!(scopes("x.nii").collect::<Vec<_>>(), vec![""]);
    }
}
