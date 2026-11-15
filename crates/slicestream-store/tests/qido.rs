#[allow(dead_code)]
#[path = "../../slicestream-core/examples/make_phantom.rs"]
mod phantom;
mod support;

use slicestream_dicomweb::{Include, Key, Level, Matcher, QidoQuery};
use slicestream_store::{
    BlobHash, DuplicatePolicy, Index, InstanceRecord, QidoPage, scan_instance,
};
use support::{Spec, instance};

struct Fixture {
    idx: Index,
    _dir: tempfile::TempDir,
}

async fn add(idx: &Index, dir: &std::path::Path, fill: u8, spec: &Spec<'_>) {
    let path = instance(dir, spec);
    let scan = scan_instance(&path).unwrap();
    let rec = InstanceRecord {
        header: scan.header,
        blob: BlobHash([fill; 32]),
        size_bytes: 100,
        source: "dimse".into(),
        calling_ae: None,
        data: scan.data,
        scanned: true,
    };
    idx.upsert_instance(&rec, DuplicatePolicy::Keep)
        .await
        .unwrap();
}

const S1: &str = "2.25.1";
const S2: &str = "2.25.2";
const S3: &str = "2.25.3";
const S4: &str = "2.25.4";

/// 3 patients, 4 studies, 7 instances. Instances are added in the order listed (ids 1..).
async fn fixture() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let idx = Index::open(&dir.path().join("index.db")).await.unwrap();
    let d = dir.path().join("src");
    let spec = |sop, series, study, modality, n| Spec {
        sop_uid: sop,
        series_uid: series,
        study_uid: study,
        modality,
        instance_number: Some(n),
        ..Spec::default()
    };
    let s1 = Spec {
        patient_name: "DOE^JANE",
        patient_id: "P1",
        study_date: "20240105",
        study_time: "093000",
        study_description: "Head",
        accession: "ACC1",
        referring: "SMITH^BOB",
        series_number: Some(1),
        ..spec("2.25.1.1.1", "2.25.1.1", S1, "CT", 1)
    };
    add(&idx, &d, 1, &s1).await;
    add(
        &idx,
        &d,
        2,
        &Spec {
            sop_uid: "2.25.1.1.2",
            instance_number: Some(2),
            ..s1.clone()
        },
    )
    .await;
    add(
        &idx,
        &d,
        3,
        &Spec {
            sop_uid: "2.25.1.2.1",
            series_uid: "2.25.1.2",
            modality: "MR",
            series_number: Some(2),
            instance_number: Some(1),
            ..s1.clone()
        },
    )
    .await;
    add(
        &idx,
        &d,
        4,
        &Spec {
            patient_name: "DOE^JOHN",
            patient_id: "P2",
            study_date: "20240215",
            study_time: "120030",
            accession: "ACC2",
            series_number: Some(1),
            ..spec("2.25.2.1.1", "2.25.2.1", S2, "MR", 1)
        },
    )
    .await;
    add(
        &idx,
        &d,
        5,
        &Spec {
            patient_name: "\u{c4}RZTIN^ANNA",
            patient_id: "P3",
            study_date: "20240320",
            study_time: "150000",
            ..spec("2.25.3.1.1", "2.25.3.1", S3, "US", 1)
        },
    )
    .await;
    add(
        &idx,
        &d,
        6,
        &Spec {
            patient_name: "DOE^JANE",
            patient_id: "P1",
            study_date: "20240601",
            study_time: "100000",
            ..spec("2.25.4.1.1", "2.25.4.1", S4, "CT", 1)
        },
    )
    .await;
    Fixture { idx, _dir: dir }
}

fn query(level: Level, matches: Vec<(Key, Matcher)>) -> QidoQuery {
    QidoQuery {
        level,
        study_uid: None,
        series_uid: None,
        matches,
        include: Include::Default,
        limit: None,
        offset: 0,
        fuzzy: false,
        ignored: vec![],
    }
}

async fn study_uids(f: &Fixture, matches: Vec<(Key, Matcher)>) -> Vec<String> {
    let page = f
        .idx
        .qido(&query(Level::Study, matches), 1000)
        .await
        .unwrap();
    page.rows.into_iter().map(|r| r.study.study_uid).collect()
}

fn single(s: &str) -> Matcher {
    Matcher::Single(s.into())
}

fn range(from: Option<&str>, to: Option<&str>) -> Matcher {
    Matcher::Range {
        from: from.map(Into::into),
        to: to.map(Into::into),
    }
}

#[tokio::test]
async fn studies_all_with_counts() {
    let f = fixture().await;
    let page = f
        .idx
        .qido(&query(Level::Study, vec![]), 1000)
        .await
        .unwrap();
    assert!(!page.more);
    assert_eq!(page.rows.len(), 4);
    let r = &page.rows[0];
    assert_eq!(r.study.study_uid, S1);
    assert_eq!(r.study.patient_id, "P1");
    assert_eq!(r.study.patient_name.as_deref(), Some("DOE^JANE"));
    assert_eq!(r.study.study_date.as_deref(), Some("20240105"));
    assert_eq!(r.study.study_time.as_deref(), Some("093000"));
    assert_eq!(r.study.accession_number.as_deref(), Some("ACC1"));
    assert_eq!(
        r.study.referring_physician_name.as_deref(),
        Some("SMITH^BOB")
    );
    assert_eq!(r.study.study_description.as_deref(), Some("Head"));
    assert_eq!(r.study.modalities, vec!["CT", "MR"]);
    assert_eq!(r.study.series_count, 2);
    assert_eq!(r.study.instance_count, 3);
    assert!(r.series.is_none() && r.instance.is_none());
    assert_eq!(page.rows[1].study.study_uid, S2);
    assert_eq!(page.rows[1].study.instance_count, 1);
}

#[tokio::test]
async fn pn_wildcard_case_insensitive() {
    let f = fixture().await;
    let m = |p: &str| vec![(Key::PatientName, Matcher::Wildcard(p.into()))];
    assert_eq!(study_uids(&f, m("doe*")).await, [S1, S2, S4]);
    assert_eq!(study_uids(&f, m("DOE^J?NE")).await, [S1, S4]);
    assert_eq!(
        study_uids(&f, vec![(Key::PatientName, single("doe^john"))]).await,
        [S2]
    );
    assert_eq!(
        study_uids(&f, vec![(Key::ReferringPhysicianName, single("smith^bob"))]).await,
        [S1]
    );
}

#[tokio::test]
async fn pn_match_folds_non_ascii() {
    let f = fixture().await;
    let got = study_uids(
        &f,
        vec![(Key::PatientName, Matcher::Wildcard("\u{e4}rzt*".into()))],
    )
    .await;
    assert_eq!(got, [S3]);
}

#[tokio::test]
async fn patient_id_exact() {
    let f = fixture().await;
    assert_eq!(
        study_uids(&f, vec![(Key::PatientId, single("P1"))]).await,
        [S1, S4]
    );
    assert!(
        study_uids(&f, vec![(Key::PatientId, single("P"))])
            .await
            .is_empty()
    );
}

#[tokio::test]
async fn study_date_range_and_open_ends() {
    let f = fixture().await;
    let d = |m| vec![(Key::StudyDate, m)];
    assert_eq!(
        study_uids(&f, d(range(Some("20240201"), Some("20240331")))).await,
        [S2, S3]
    );
    assert_eq!(
        study_uids(&f, d(range(Some("20240320"), None))).await,
        [S3, S4]
    );
    assert_eq!(
        study_uids(&f, d(range(None, Some("20240215")))).await,
        [S1, S2]
    );
    assert_eq!(study_uids(&f, d(single("20240601"))).await, [S4]);
}

#[tokio::test]
async fn study_time_range_padding() {
    let f = fixture().await;
    let t = |m| vec![(Key::StudyTime, m)];
    // The upper bound 1200 stands for 120059.999999, so 120030 is inside.
    assert_eq!(
        study_uids(&f, t(range(Some("1000"), Some("1200")))).await,
        [S2, S4]
    );
    assert_eq!(
        study_uids(&f, t(range(Some("095959"), Some("120029")))).await,
        [S4]
    );
    assert_eq!(study_uids(&f, t(range(Some("1500"), None))).await, [S3]);
    // `09` as an upper bound is 09:59:59.999999: 093000 is inside.
    assert_eq!(study_uids(&f, t(range(None, Some("09")))).await, [S1]);
    assert!(
        study_uids(&f, t(range(None, Some("0929"))))
            .await
            .is_empty()
    );
    assert_eq!(study_uids(&f, t(single("12"))).await, [S2]);
}

#[tokio::test]
async fn modalities_in_study_list() {
    let f = fixture().await;
    let m = |l: &[&str]| {
        vec![(
            Key::ModalitiesInStudy,
            Matcher::List(l.iter().map(|s| s.to_string()).collect()),
        )]
    };
    assert_eq!(study_uids(&f, m(&["MR", "US"])).await, [S1, S2, S3]);
    assert_eq!(study_uids(&f, m(&["US"])).await, [S3]);
    assert_eq!(
        study_uids(&f, vec![(Key::ModalitiesInStudy, single("CT"))]).await,
        [S1, S4]
    );
    // A matching study still reports all of its modalities.
    let page = f
        .idx
        .qido(&query(Level::Study, m(&["MR"])), 1000)
        .await
        .unwrap();
    assert_eq!(page.rows[0].study.modalities, vec!["CT", "MR"]);
}

#[tokio::test]
async fn uid_list() {
    let f = fixture().await;
    let l = |v: &[&str]| Matcher::List(v.iter().map(|s| s.to_string()).collect());
    assert_eq!(
        study_uids(&f, vec![(Key::StudyInstanceUid, l(&[S2, S4, "2.25.9"]))]).await,
        [S2, S4]
    );
    // Empty entries are ignored; an all-empty list is universal.
    assert_eq!(
        study_uids(&f, vec![(Key::StudyInstanceUid, l(&[S3, ""]))]).await,
        [S3]
    );
    assert_eq!(
        study_uids(&f, vec![(Key::StudyInstanceUid, l(&["", ""]))])
            .await
            .len(),
        4
    );
}

#[tokio::test]
async fn series_with_higher_level_key() {
    let f = fixture().await;
    let q = query(Level::Series, vec![(Key::PatientId, single("P1"))]);
    let page = f.idx.qido(&q, 1000).await.unwrap();
    let uids: Vec<_> = page
        .rows
        .iter()
        .map(|r| r.series.as_ref().unwrap().series_uid.as_str())
        .collect();
    assert_eq!(uids, ["2.25.1.1", "2.25.1.2", "2.25.4.1"]);
    let s = page.rows[0].series.as_ref().unwrap();
    assert_eq!(s.modality.as_deref(), Some("CT"));
    assert_eq!(s.series_number, Some(1));
    assert_eq!(s.instance_count, 2);
    assert_eq!(page.rows[0].study.study_uid, S1);

    let mut q = query(Level::Series, vec![(Key::Modality, single("MR"))]);
    q.study_uid = Some(S1.into());
    let page = f.idx.qido(&q, 1000).await.unwrap();
    assert_eq!(page.rows.len(), 1);
    assert_eq!(page.rows[0].series.as_ref().unwrap().series_uid, "2.25.1.2");

    // Series Number is an integer key.
    let q = query(Level::Series, vec![(Key::SeriesNumber, single("2"))]);
    assert_eq!(f.idx.qido(&q, 1000).await.unwrap().rows.len(), 1);
    let q = query(Level::Series, vec![(Key::SeriesNumber, single("two"))]);
    assert!(f.idx.qido(&q, 1000).await.unwrap().rows.is_empty());
    let q = query(
        Level::Series,
        vec![(Key::SeriesNumber, Matcher::Wildcard("*".into()))],
    );
    assert_eq!(f.idx.qido(&q, 1000).await.unwrap().rows.len(), 3);
}

#[tokio::test]
async fn request_attribute_sequence_matches_only_universal() {
    let f = fixture().await;
    let q = query(
        Level::Series,
        vec![(Key::RequestAttributeSequence, Matcher::Universal)],
    );
    assert_eq!(f.idx.qido(&q, 1000).await.unwrap().rows.len(), 5);
    let q = query(
        Level::Series,
        vec![(Key::RequestAttributeSequence, single("X"))],
    );
    assert!(f.idx.qido(&q, 1000).await.unwrap().rows.is_empty());
}

#[tokio::test]
async fn instances_under_series() {
    let f = fixture().await;
    let mut q = query(Level::Instance, vec![]);
    q.study_uid = Some(S1.into());
    q.series_uid = Some("2.25.1.1".into());
    let page = f.idx.qido(&q, 1000).await.unwrap();
    assert_eq!(page.rows.len(), 2);
    let i = page.rows[0].instance.as_ref().unwrap();
    assert_eq!(i.sop_uid, "2.25.1.1.1");
    assert_eq!(i.instance_number, Some(1));
    assert_eq!(i.number_of_frames, 1);
    assert!(i.frame_info.is_some());
    assert!(!i.sop_class_uid.is_empty());
    assert_eq!(page.rows[0].series.as_ref().unwrap().series_uid, "2.25.1.1");

    // All instances of the study, then narrowed by Instance Number.
    q.series_uid = None;
    assert_eq!(f.idx.qido(&q, 1000).await.unwrap().rows.len(), 3);
    q.matches = vec![(Key::InstanceNumber, single("2"))];
    let page = f.idx.qido(&q, 1000).await.unwrap();
    assert_eq!(page.rows.len(), 1);
    assert_eq!(
        page.rows[0].instance.as_ref().unwrap().sop_uid,
        "2.25.1.1.2"
    );
    q.matches = vec![(Key::SopInstanceUid, single("2.25.1.2.1"))];
    assert_eq!(f.idx.qido(&q, 1000).await.unwrap().rows.len(), 1);
}

fn uids(p: &QidoPage) -> Vec<&str> {
    p.rows.iter().map(|r| r.study.study_uid.as_str()).collect()
}

#[tokio::test]
async fn limit_offset_more() {
    let f = fixture().await;
    let mut q = query(Level::Study, vec![]);
    q.limit = Some(2);
    let p = f.idx.qido(&q, 1000).await.unwrap();
    assert_eq!(uids(&p), [S1, S2]);
    assert!(p.more);
    q.offset = 2;
    let p = f.idx.qido(&q, 1000).await.unwrap();
    assert_eq!(uids(&p), [S3, S4]);
    assert!(!p.more);
    q.offset = 3;
    let p = f.idx.qido(&q, 1000).await.unwrap();
    assert_eq!(uids(&p), [S4]);
    assert!(!p.more);

    // The server maximum caps the client limit.
    q.limit = Some(50);
    q.offset = 0;
    let p = f.idx.qido(&q, 3).await.unwrap();
    assert_eq!(p.rows.len(), 3);
    assert!(p.more);

    // Limit 0 returns no rows; more says whether anything matches.
    q.limit = Some(0);
    let p = f.idx.qido(&q, 1000).await.unwrap();
    assert!(p.rows.is_empty() && p.more);
    q.matches = vec![(Key::PatientId, single("nobody"))];
    let p = f.idx.qido(&q, 1000).await.unwrap();
    assert!(p.rows.is_empty() && !p.more);
}

#[tokio::test]
async fn glob_metacharacters_are_literal() {
    let dir = tempfile::tempdir().unwrap();
    let idx = Index::open(&dir.path().join("index.db")).await.unwrap();
    let d = dir.path().join("src");
    for (n, (name, study)) in [("[A]X", "2.25.7"), ("AX", "2.25.8")]
        .into_iter()
        .enumerate()
    {
        add(
            &idx,
            &d,
            n as u8 + 1,
            &Spec {
                patient_name: name,
                patient_id: "G",
                study_uid: study,
                series_uid: &format!("{study}.1"),
                sop_uid: &format!("{study}.1.1"),
                modality: "CT",
                ..Spec::default()
            },
        )
        .await;
    }
    let f = Fixture { idx, _dir: dir };
    // `[A]*` must match the literal name `[A]X`, not any name starting with A.
    let got = study_uids(
        &f,
        vec![(Key::PatientName, Matcher::Wildcard("[A]*".into()))],
    )
    .await;
    assert_eq!(got, ["2.25.7"]);
    let got = study_uids(
        &f,
        vec![(Key::PatientName, Matcher::Wildcard("?A?*".into()))],
    )
    .await;
    assert_eq!(got, ["2.25.7"]);
}

#[tokio::test]
async fn representative_is_lowest_id() {
    let f = fixture().await;
    let page = f
        .idx
        .qido(&query(Level::Study, vec![]), 1000)
        .await
        .unwrap();
    let reps: Vec<_> = page.rows.iter().map(|r| r.representative).collect();
    assert_eq!(reps, [1, 4, 5, 6]);
    let page = f
        .idx
        .qido(&query(Level::Series, vec![]), 1000)
        .await
        .unwrap();
    let reps: Vec<_> = page.rows.iter().map(|r| r.representative).collect();
    assert_eq!(reps, [1, 3, 4, 5, 6]);
    let page = f
        .idx
        .qido(&query(Level::Instance, vec![]), 1000)
        .await
        .unwrap();
    let reps: Vec<_> = page.rows.iter().map(|r| r.representative).collect();
    assert_eq!(reps, [1, 2, 3, 4, 5, 6]);
}
