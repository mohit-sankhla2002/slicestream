//! QIDO-RS, `[dicomweb]` config, CORS on the DICOMweb paths and the metadata
//! backfill, against the full server on an ephemeral port. Every instance is
//! synthesized here (no real patient data).

use std::io::Read;
use std::path::Path;
use std::time::{Duration, Instant};

use dicom_core::VR;
use dicom_dictionary_std::tags;
use serde_json::Value;
use slicestream_server::config::Config;

mod support;
use support::*;

const DICOM_JSON: &str = "application/dicom+json";
const ORIGIN: &str = "https://viewer.example";
const FUZZY_TEXT: &str =
    "The fuzzymatching parameter is not supported. Only literal matching has been performed.";

/// Study A: patient 1, SMITH^JOHN, 2026-01-05; series 1 CT (2 instances,
/// Series Date only in the file), series 2 MR (1 instance).
/// Study B: patient 2, Smithson^Anna, 2026-02-10; series 1 CT (1 instance).
/// Study C: patient 3, JONES^BOB, 2025-12-31; series 1 US (1 instance).
fn corpus() -> Vec<Vec<u8>> {
    let a = |series: u32, inst: u32, modality: &str| {
        dicom_with(
            1,
            1,
            series,
            inst,
            &[
                (tags::PATIENT_NAME, VR::PN, "SMITH^JOHN"),
                (tags::STUDY_DATE, VR::DA, "20260105"),
                (tags::MODALITY, VR::CS, modality),
                (tags::ACCESSION_NUMBER, VR::SH, "ACC1"),
                (tags::REFERRING_PHYSICIAN_NAME, VR::PN, "DOC^ONE"),
                (tags::PATIENT_BIRTH_DATE, VR::DA, "19700101"),
                (tags::PATIENT_AGE, VR::AS, "056Y"),
                (
                    tags::SERIES_DATE,
                    VR::DA,
                    if series == 1 { "20260106" } else { "" },
                ),
            ],
        )
    };
    vec![
        a(1, 1, "CT"),
        a(1, 2, "CT"),
        a(2, 1, "MR"),
        dicom_with(
            2,
            2,
            1,
            1,
            &[
                (tags::PATIENT_NAME, VR::PN, "Smithson^Anna"),
                (tags::STUDY_DATE, VR::DA, "20260210"),
            ],
        ),
        dicom_with(
            3,
            3,
            1,
            1,
            &[
                (tags::PATIENT_NAME, VR::PN, "JONES^BOB"),
                (tags::STUDY_DATE, VR::DA, "20251231"),
                (tags::MODALITY, VR::CS, "US"),
            ],
        ),
    ]
}

async fn server_with(configure: impl FnOnce(&mut Config)) -> TestServer {
    let srv = TestServer::start(configure).await;
    srv.stow(&corpus()).await;
    srv
}

async fn server() -> TestServer {
    server_with(|_| {}).await
}

/// GET `path`, expect 200 DICOM JSON, return the array.
async fn qido(srv: &TestServer, path: &str) -> Vec<Value> {
    let r = srv.get(path, &[]).await;
    assert_eq!(r.status, 200, "{path}: {}", r.text());
    assert_eq!(r.header("content-type"), Some(DICOM_JSON), "{path}");
    r.json().as_array().cloned().expect("JSON array")
}

/// First value of attribute `tag` (as JSON).
fn val<'a>(obj: &'a Value, tag: &str) -> &'a Value {
    &obj[tag]["Value"][0]
}

fn uids(rows: &[Value], tag: &str) -> Vec<String> {
    rows.iter()
        .map(|r| val(r, tag).as_str().unwrap().to_owned())
        .collect()
}

fn a() -> String {
    study_uid(1, 1)
}
fn b() -> String {
    study_uid(2, 2)
}
fn c() -> String {
    study_uid(3, 3)
}

#[tokio::test]
async fn studies_lists_with_counts_and_retrieve_url() {
    let srv = server().await;
    let rows = qido(&srv, "/dicom-web/studies").await;
    assert_eq!(uids(&rows, "0020000D"), [a(), b(), c()]);
    let s = &rows[0];
    assert_eq!(s["00201206"], serde_json::json!({"vr": "IS", "Value": [2]}));
    assert_eq!(s["00201208"], serde_json::json!({"vr": "IS", "Value": [3]}));
    assert_eq!(
        s["00081190"],
        serde_json::json!({"vr": "UR", "Value": [format!("http://{}/dicom-web/studies/{}", srv.addr(), a())]})
    );
    assert_eq!(val(s, "00080056"), "ONLINE");
    assert_eq!(val(s, "00080005"), "ISO_IR 192");
    assert_eq!(s["00100010"]["vr"], "PN");
    assert_eq!(val(s, "00100010")["Alphabetic"], "SMITH^JOHN");
    assert_eq!(val(s, "00100020"), "P1");
    assert_eq!(val(s, "00080020"), "20260105");
    assert_eq!(val(s, "00080050"), "ACC1");
    assert_eq!(val(s, "00080090")["Alphabetic"], "DOC^ONE");
    assert_eq!(val(s, "00100030"), "19700101");
    // Study Description is not a PS3.18 default; it comes on request.
    assert!(s.get("00081030").is_none());
    // Attributes are in tag order (PS3.18 Annex F).
    let keys: Vec<&String> = s.as_object().unwrap().keys().collect();
    let mut sorted = keys.clone();
    sorted.sort();
    assert_eq!(keys, sorted);
    // Series-level attributes are not study results.
    assert!(s.get("0020000E").is_none());
    assert_eq!(rows[1]["00201206"]["Value"][0], 1);
    srv.stop().await;
}

#[tokio::test]
async fn ohif_study_list_query() {
    let srv = server().await;
    let rows = qido(
        &srv,
        "/dicom-web/studies?limit=101&offset=0&fuzzymatching=false\
         &includefield=00081030%2C00080060%2C00080090%2C00100030",
    )
    .await;
    assert_eq!(uids(&rows, "0020000D"), [a(), b(), c()]);
    assert_eq!(val(&rows[0], "00081030"), "Study 1");
    // Modality is series level: it comes from the representative instance.
    assert_eq!(
        rows[0]["00080060"],
        serde_json::json!({"vr": "CS", "Value": ["CT"]})
    );
    assert_eq!(val(&rows[2], "00080060"), "US");
    assert_eq!(val(&rows[0], "00080090")["Alphabetic"], "DOC^ONE");
    assert_eq!(val(&rows[0], "00100030"), "19700101");
    srv.stop().await;
}

#[tokio::test]
async fn patient_name_wildcard_case_insensitive() {
    let srv = server().await;
    let rows = qido(&srv, "/dicom-web/studies?PatientName=smith*").await;
    assert_eq!(uids(&rows, "0020000D"), [a(), b()]);
    let rows = qido(&srv, "/dicom-web/studies?00100010=SMITHSON*").await;
    assert_eq!(uids(&rows, "0020000D"), [b()]);
    srv.stop().await;
}

#[tokio::test]
async fn study_date_range() {
    let srv = server().await;
    let rows = qido(&srv, "/dicom-web/studies?StudyDate=20260101-20260131").await;
    assert_eq!(uids(&rows, "0020000D"), [a()]);
    let rows = qido(&srv, "/dicom-web/studies?StudyDate=20260101-").await;
    assert_eq!(uids(&rows, "0020000D"), [a(), b()]);
    let rows = qido(&srv, "/dicom-web/studies?StudyDate=-20251231").await;
    assert_eq!(uids(&rows, "0020000D"), [c()]);
    srv.stop().await;
}

#[tokio::test]
async fn modalities_in_study() {
    let srv = server().await;
    let rows = qido(&srv, "/dicom-web/studies?ModalitiesInStudy=MR").await;
    assert_eq!(uids(&rows, "0020000D"), [a()]);
    assert_eq!(
        rows[0]["00080061"],
        serde_json::json!({"vr": "CS", "Value": ["CT", "MR"]})
    );
    srv.stop().await;
}

#[tokio::test]
async fn series_under_study() {
    let srv = server().await;
    let rows = qido(&srv, &format!("/dicom-web/studies/{}/series", a())).await;
    assert_eq!(
        uids(&rows, "0020000E"),
        [series_uid(1, 1, 1), series_uid(1, 1, 2)]
    );
    assert_eq!(uids(&rows, "00080060"), ["CT", "MR"]);
    assert_eq!(rows[0]["00201209"]["Value"][0], 2);
    assert_eq!(rows[1]["00201209"]["Value"][0], 1);
    assert_eq!(val(&rows[0], "0020000D"), a().as_str());
    assert_eq!(
        val(&rows[0], "00081190"),
        format!(
            "http://{}/dicom-web/studies/{}/series/{}",
            srv.addr(),
            a(),
            series_uid(1, 1, 1)
        )
        .as_str()
    );
    // Study attributes come only when the target is all series.
    assert!(rows[0].get("00100010").is_none());
    let rows = qido(
        &srv,
        &format!("/dicom-web/studies/{}/series?Modality=MR", a()),
    )
    .await;
    assert_eq!(uids(&rows, "0020000E"), [series_uid(1, 1, 2)]);
    srv.stop().await;
}

#[tokio::test]
async fn series_at_root_with_patient_id() {
    let srv = server().await;
    let rows = qido(&srv, "/dicom-web/series?PatientID=P2").await;
    assert_eq!(uids(&rows, "0020000E"), [series_uid(2, 2, 1)]);
    assert_eq!(val(&rows[0], "00100020"), "P2");
    assert_eq!(val(&rows[0], "00100010")["Alphabetic"], "Smithson^Anna");
    assert_eq!(val(&rows[0], "0020000D"), b().as_str());
    srv.stop().await;
}

#[tokio::test]
async fn instances_under_series() {
    let srv = server().await;
    let path = format!(
        "/dicom-web/studies/{}/series/{}/instances",
        a(),
        series_uid(1, 1, 1)
    );
    let rows = qido(&srv, &path).await;
    assert_eq!(uids(&rows, "00080018"), [sop(1, 1, 1, 1), sop(1, 1, 1, 2)]);
    let i = &rows[1];
    assert_eq!(val(i, "00080016"), "1.2.840.10008.5.1.4.1.1.2");
    assert_eq!(i["00200013"], serde_json::json!({"vr": "IS", "Value": [2]}));
    assert_eq!(i["00280010"], serde_json::json!({"vr": "US", "Value": [4]}));
    assert_eq!(val(i, "00280011"), 4);
    assert_eq!(val(i, "00280100"), 16);
    assert_eq!(i["00280008"], serde_json::json!({"vr": "IS", "Value": [1]}));
    assert_eq!(val(i, "00080056"), "ONLINE");
    assert_eq!(
        val(i, "00081190"),
        format!(
            "http://{}/dicom-web/studies/{}/series/{}/instances/{}",
            srv.addr(),
            a(),
            series_uid(1, 1, 1),
            sop(1, 1, 1, 2)
        )
        .as_str()
    );
    assert_eq!(val(i, "0020000E"), series_uid(1, 1, 1).as_str());

    // All instances, by a UID list.
    let rows = qido(
        &srv,
        &format!(
            "/dicom-web/instances?SOPInstanceUID={},{}",
            sop(3, 3, 1, 1),
            sop(1, 1, 2, 1)
        ),
    )
    .await;
    assert_eq!(uids(&rows, "00080018"), [sop(1, 1, 2, 1), sop(3, 3, 1, 1)]);
    // Study and series attributes come with all-instances results.
    assert_eq!(val(&rows[0], "00080060"), "MR");
    assert_eq!(val(&rows[1], "00100010")["Alphabetic"], "JONES^BOB");

    let rows = qido(&srv, &format!("/dicom-web/studies/{}/instances", a())).await;
    assert_eq!(rows.len(), 3);
    srv.stop().await;
}

#[tokio::test]
async fn includefield_from_metadata() {
    let srv = server().await;
    for field in ["00080021", "SeriesDate"] {
        let rows = qido(
            &srv,
            &format!("/dicom-web/studies/{}/series?includefield={field}", a()),
        )
        .await;
        assert_eq!(
            rows[0]["00080021"],
            serde_json::json!({"vr": "DA", "Value": ["20260106"]}),
            "{field}"
        );
        // Series 2 has an empty Series Date in its file.
        assert!(rows[1]["00080021"].get("Value").is_none(), "{field}");
    }
    // A study attribute the index does not hold comes from the file.
    let rows = qido(
        &srv,
        "/dicom-web/studies?includefield=PatientAge&PatientID=P1",
    )
    .await;
    assert_eq!(val(&rows[0], "00101010"), "056Y");
    srv.stop().await;
}

#[tokio::test]
async fn includefield_all_study_level() {
    let srv = server().await;
    let rows = qido(
        &srv,
        &format!(
            "/dicom-web/studies?StudyInstanceUID={}&includefield=all",
            a()
        ),
    )
    .await;
    assert_eq!(rows.len(), 1);
    let s = &rows[0];
    // Patient Study module, from the representative instance.
    assert_eq!(val(s, "00101010"), "056Y");
    // Computed values win over the file's.
    assert_eq!(val(s, "00080005"), "ISO_IR 192");
    assert_eq!(val(s, "00201208"), 3);
    // Not Patient / General Study / Patient Study attributes.
    for tag in ["00080021", "00280010", "7FE00010", "00080018"] {
        assert!(s.get(tag).is_none(), "{tag}");
    }
    srv.stop().await;
}

#[tokio::test]
async fn includefield_all_instance_level_drops_bulk() {
    let srv = server().await;
    let rows = qido(
        &srv,
        &format!(
            "/dicom-web/instances?SOPInstanceUID={}&includefield=all",
            sop(1, 1, 1, 1)
        ),
    )
    .await;
    let i = &rows[0];
    assert_eq!(val(i, "00280004"), "MONOCHROME2");
    assert_eq!(val(i, "00080021"), "20260106");
    assert!(i.get("7FE00010").is_none(), "bulk Pixel Data left out");
    assert!(!String::from_utf8_lossy(&serde_json::to_vec(i).unwrap()).contains("BulkDataURI"));
    srv.stop().await;
}

#[tokio::test]
async fn fuzzymatching_warning_header() {
    let srv = server().await;
    let r = srv.get("/dicom-web/studies?fuzzymatching=true", &[]).await;
    assert_eq!(r.status, 200);
    let w = r.header_all("warning");
    assert_eq!(w.len(), 1, "{w:?}");
    // PS3.18 8.3.4.2: `<service>` is the base URI.
    assert_eq!(w[0], format!("299 http://{}: \"{FUZZY_TEXT}\"", srv.addr()));
    assert_eq!(r.json().as_array().unwrap().len(), 3);
    let r = srv.get("/dicom-web/studies?fuzzymatching=false", &[]).await;
    assert!(r.header("warning").is_none());
    srv.stop().await;
}

#[tokio::test]
async fn limit_more_warning_header() {
    let srv = server().await;
    let r = srv.get("/dicom-web/studies?limit=2", &[]).await;
    assert_eq!(uids(r.json().as_array().unwrap(), "0020000D"), [a(), b()]);
    let w = r.header("warning").expect("warning");
    assert!(w.starts_with("299 "), "{w}");
    assert!(
        w.contains("additional results that can be requested"),
        "{w}"
    );
    let r = srv.get("/dicom-web/studies?limit=2&offset=2", &[]).await;
    assert_eq!(uids(r.json().as_array().unwrap(), "0020000D"), [c()]);
    assert!(r.header("warning").is_none());
    srv.stop().await;

    // max_results is the default and the cap.
    let srv = server_with(|c| c.dicomweb.max_results = 2).await;
    for path in ["/dicom-web/studies", "/dicom-web/studies?limit=5"] {
        let r = srv.get(path, &[]).await;
        assert_eq!(r.json().as_array().unwrap().len(), 2, "{path}");
        assert!(r.header("warning").is_some(), "{path}");
    }
    srv.stop().await;
}

#[tokio::test]
async fn unsupported_key_ignored_with_warning() {
    let srv = server().await;
    // PS3.18 8.3: unsupported query parameters are ignored. Modality is a
    // series key, so a study query ignores it too.
    let r = srv
        .get("/dicom-web/studies?Foo=bar&Modality=MR&PatientID=P1", &[])
        .await;
    assert_eq!(r.status, 200);
    assert_eq!(uids(r.json().as_array().unwrap(), "0020000D"), [a()]);
    let w = r.header_all("warning");
    assert_eq!(w.len(), 1, "{w:?}");
    assert!(w[0].starts_with("299 "), "{}", w[0]);
    assert!(
        w[0].contains("Foo") && w[0].contains("Modality"),
        "{}",
        w[0]
    );
    assert!(!w[0].contains("PatientID"), "{}", w[0]);

    // A name with quotes or control bytes cannot break the header.
    let r = srv.get("/dicom-web/studies?a%22b%5Cc%0D%0Ad=1", &[]).await;
    assert_eq!(r.status, 200);
    let w = r.header("warning").unwrap();
    assert_eq!(w.matches('"').count(), 2, "{w}");

    // An invalid value of a supported key is still a 400 naming it.
    for (q, name) in [
        ("StudyDate=2026", "StudyDate"),
        ("limit=x", "limit"),
        ("includefield=NotAKeyword", "includefield"),
    ] {
        let r = srv.get(&format!("/dicom-web/studies?{q}"), &[]).await;
        assert_eq!(r.status, 400, "{q}");
        assert!(r.header("content-type").unwrap().starts_with("text/plain"));
        assert!(r.text().contains(name), "{q}: {}", r.text());
    }
    srv.stop().await;
}

#[tokio::test]
async fn uid_list_over_1000_400() {
    let srv = server().await;
    let list = |n: usize| {
        let mut v: Vec<String> = (0..n - 1).map(|i| format!("1.2.{i}")).collect();
        v.push(a());
        v.join(",")
    };
    let r = srv
        .get(
            &format!("/dicom-web/studies?StudyInstanceUID={}", list(1000)),
            &[],
        )
        .await;
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.json().as_array().unwrap().len(), 1);
    let r = srv
        .get(
            &format!("/dicom-web/studies?StudyInstanceUID={}", list(1001)),
            &[],
        )
        .await;
    assert_eq!(r.status, 400);
    assert!(r.header("content-type").unwrap().starts_with("text/plain"));
    assert!(r.text().contains("1000"), "{}", r.text());
    srv.stop().await;
}

#[tokio::test]
async fn xml_only_406() {
    let srv = server().await;
    let r = srv
        .get("/dicom-web/studies", &[("Accept", "application/dicom+xml")])
        .await;
    assert_eq!(r.status, 406);
    for accept in [
        "application/dicom+json",
        "application/json",
        "*/*",
        "application/dicom+xml, application/json;q=0.5",
    ] {
        let r = srv.get("/dicom-web/studies", &[("Accept", accept)]).await;
        assert_eq!(r.status, 200, "{accept}");
    }
    srv.stop().await;
}

#[tokio::test]
async fn empty_result_200_array() {
    let srv = server().await;
    let r = srv.get("/dicom-web/studies?PatientID=NOPE", &[]).await;
    assert_eq!(r.status, 200);
    assert_eq!(r.header("content-type"), Some(DICOM_JSON));
    assert_eq!(r.json(), serde_json::json!([]));
    let r = srv.get("/dicom-web/studies/1.2.3/series", &[]).await;
    assert_eq!(r.json(), serde_json::json!([]));
    srv.stop().await;
}

#[tokio::test]
async fn dicomweb_disabled_404() {
    let srv = TestServer::start(|c| c.dicomweb.enabled = false).await;
    // STOW-RS is not affected.
    srv.stow(&corpus()).await;
    for path in [
        "/dicom-web/studies".to_owned(),
        "/dicom-web/series".into(),
        "/dicom-web/instances".into(),
        format!("/dicom-web/studies/{}/series", a()),
        format!("/dicom-web/studies/{}/instances", a()),
        format!(
            "/dicom-web/studies/{}/series/{}/instances",
            a(),
            series_uid(1, 1, 1)
        ),
    ] {
        let r = srv.get(&path, &[]).await;
        assert_eq!(r.status, 404, "{path}");
    }
    srv.stop().await;
}

/// GET and STOW-RS POST share `/dicom-web/studies`: GET answers QIDO with
/// CORS, preflight succeeds, POST keeps its unlimited body and no CORS.
#[tokio::test]
async fn cors_preflight_allows_accept() {
    let srv = server_with(|c| c.http.cors_allowed_origins = vec![ORIGIN.into()]).await;
    let r = http_options(
        srv.addr(),
        "/dicom-web/studies",
        &[
            ("Origin", ORIGIN),
            ("Access-Control-Request-Method", "GET"),
            ("Access-Control-Request-Headers", "accept"),
        ],
    )
    .await;
    assert_eq!(r.status, 200, "{:?}", r.headers);
    assert_eq!(r.header("access-control-allow-origin"), Some(ORIGIN));
    let headers = r.header("access-control-allow-headers").unwrap();
    assert!(headers.to_ascii_lowercase().contains("accept"), "{headers}");
    let methods = r.header("access-control-allow-methods").unwrap();
    assert!(
        methods.contains("GET") && !methods.contains("POST"),
        "{methods}"
    );

    let r = srv
        .get(
            "/dicom-web/studies?fuzzymatching=true",
            &[
                ("Origin", ORIGIN),
                (
                    "Accept",
                    "multipart/related; type=\"application/dicom+json\", application/dicom+json",
                ),
            ],
        )
        .await;
    assert_eq!(r.status, 200);
    assert_eq!(r.header("access-control-allow-origin"), Some(ORIGIN));
    let exposed = r
        .header("access-control-expose-headers")
        .unwrap()
        .to_ascii_lowercase();
    assert!(exposed.contains("warning"), "{exposed}");
    assert!(exposed.contains("x-slicestream-frame-info"), "{exposed}");
    assert_eq!(r.json().as_array().unwrap().len(), 3);

    // A different origin gets nothing.
    let r = srv
        .get("/dicom-web/studies", &[("Origin", "https://evil.example")])
        .await;
    assert_eq!(r.header("access-control-allow-origin"), None);

    // POST on the shared path: STOW-RS, no CORS, no 2 MiB body limit.
    let big = dicom_sized(4, 4, 1, 1, 3 << 20);
    let r = stow(
        srv.addr(),
        "/dicom-web/studies",
        &[big],
        &[("Origin", ORIGIN)],
    )
    .await;
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.header("access-control-allow-origin"), None);
    let r = stow(
        srv.addr(),
        &format!("/dicom-web/studies/{}", study_uid(4, 5)),
        &[dicom(4, 5, 1, 1)],
        &[("Origin", ORIGIN)],
    )
    .await;
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.header("access-control-allow-origin"), None);
    assert_eq!(qido(&srv, "/dicom-web/studies").await.len(), 5);
    srv.stop().await;
}

#[tokio::test]
async fn no_cors_without_origin_config() {
    let srv = server().await;
    let r = srv.get("/dicom-web/studies", &[("Origin", ORIGIN)]).await;
    assert_eq!(r.status, 200);
    assert_eq!(r.header("access-control-allow-origin"), None);
    let r = http_options(
        srv.addr(),
        "/dicom-web/studies",
        &[
            ("Origin", ORIGIN),
            ("Access-Control-Request-Method", "GET"),
            ("Access-Control-Request-Headers", "accept"),
        ],
    )
    .await;
    assert_eq!(r.header("access-control-allow-origin"), None);
    srv.stop().await;
}

#[tokio::test]
async fn json_gzip_when_accepted() {
    let srv = server().await;
    let plain = srv.get("/dicom-web/studies", &[]).await;
    assert_eq!(plain.header("content-encoding"), None);
    let r = srv
        .get("/dicom-web/studies", &[("Accept-Encoding", "gzip")])
        .await;
    assert_eq!(r.status, 200);
    assert_eq!(r.header("content-encoding"), Some("gzip"));
    assert_eq!(r.header("content-type"), Some(DICOM_JSON));
    let mut json = Vec::new();
    flate2::read::GzDecoder::new(&r.body[..])
        .read_to_end(&mut json)
        .unwrap();
    assert_eq!(json, plain.body);
    let r = srv
        .get("/dicom-web/studies", &[("Accept-Encoding", "br")])
        .await;
    assert_eq!(r.header("content-encoding"), Some("br"));
    // Plain-text errors are not compressed.
    let r = srv
        .get(
            "/dicom-web/studies",
            &[
                ("Accept-Encoding", "gzip"),
                ("Accept", "application/dicom+xml"),
            ],
        )
        .await;
    assert_eq!(r.status, 406);
    assert!(r.body.len() > 32, "{}", r.text());
    assert_eq!(r.header("content-encoding"), None);
    srv.stop().await;
}

#[tokio::test]
async fn stow_retrieve_url_uses_public_url() {
    let srv =
        server_with(|c| c.http.public_url = Some("https://pacs.example.org/pacs/".into())).await;
    let r = srv.stow(&[dicom(5, 5, 1, 1)]).await;
    let json = r.json();
    assert_eq!(
        json["00081190"]["Value"][0],
        format!(
            "https://pacs.example.org/pacs/dicom-web/studies/{}",
            study_uid(5, 5)
        )
    );
    assert_eq!(
        json["00081199"]["Value"][0]["00081190"]["Value"][0],
        format!(
            "https://pacs.example.org/pacs/dicom-web/studies/{}/series/{}/instances/{}",
            study_uid(5, 5),
            series_uid(5, 5, 1),
            sop(5, 5, 1, 1)
        )
    );
    let rows = qido(&srv, "/dicom-web/studies?PatientID=P5").await;
    assert_eq!(
        val(&rows[0], "00081190"),
        format!(
            "https://pacs.example.org/pacs/dicom-web/studies/{}",
            study_uid(5, 5)
        )
        .as_str()
    );
    srv.stop().await;
}

/// Without `public_url`, Retrieve URLs use the Host header.
#[tokio::test]
async fn stow_retrieve_url_from_host() {
    let srv = TestServer::start(|_| {}).await;
    let r = srv.stow(&[dicom(6, 6, 1, 1)]).await;
    assert_eq!(
        r.json()["00081190"]["Value"][0],
        format!(
            "http://{}/dicom-web/studies/{}",
            srv.addr(),
            study_uid(6, 6)
        )
    );
    srv.stop().await;
}

fn write(dir: &Path, text: &str) -> std::path::PathBuf {
    let p = dir.join("c.toml");
    std::fs::write(&p, text).unwrap();
    p
}

#[tokio::test]
async fn config_rejects_bad_public_url_and_zero_max_results() {
    let dir = tempfile::tempdir().unwrap();
    for (text, name) in [
        ("[http]\npublic_url = \"pacs.example.org\"\n", "public_url"),
        (
            "[http]\npublic_url = \"ftp://pacs.example.org\"\n",
            "public_url",
        ),
        ("[http]\npublic_url = \"https://\"\n", "public_url"),
        ("[dicomweb]\nmax_results = 0\n", "max_results"),
        ("[dicomweb]\nmax_rows = 10\n", "max_rows"),
    ] {
        let err = Config::load(Some(&write(dir.path(), text))).unwrap_err();
        assert!(format!("{err:#}").contains(name), "{text}: {err:#}");
    }
    let ok = Config::load(Some(&write(
        dir.path(),
        "[http]\npublic_url = \"https://pacs.example.org/pacs\"\n\
         [dicomweb]\nenabled = false\nmax_results = 50\n",
    )))
    .unwrap();
    assert_eq!(
        ok.http.public_url.as_deref(),
        Some("https://pacs.example.org/pacs")
    );
    assert!(!ok.dicomweb.enabled);
    assert_eq!(ok.dicomweb.max_results, 50);
    let d = Config::default().dicomweb;
    assert!(d.enabled);
    assert_eq!(d.max_results, 1000);

    // `start` checks a config built in code too.
    let mut config = Config::default();
    config.storage.root = dir.path().join("store");
    config.http.addr = "127.0.0.1:0".parse().unwrap();
    config.dimse.enabled = false;
    config.dicomweb.max_results = 0;
    let err = slicestream_server::start(config, tokio_util::sync::CancellationToken::new())
        .await
        .err()
        .expect("start rejects max_results = 0");
    assert!(format!("{err:#}").contains("max_results"), "{err:#}");
}

async fn index_pool(root: &Path) -> sqlx::SqlitePool {
    sqlx::SqlitePool::connect(&format!("sqlite://{}", root.join("index.sqlite").display()))
        .await
        .unwrap()
}

/// An old store (rows never scanned) is scanned in the background after
/// startup, and QIDO answers from metadata meanwhile.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn startup_backfills_unscanned_instances() {
    let srv = server().await;
    let root = srv.root();
    let dir = srv.stop_keep_dir().await;
    let pool = index_pool(&root).await;
    for q in [
        "DELETE FROM instance_data",
        "UPDATE instances SET scan_version = 0",
    ] {
        sqlx::query(q).execute(&pool).await.unwrap();
    }

    let srv = TestServer::start_in(dir, |_| {}).await;
    let rows = qido(
        &srv,
        &format!("/dicom-web/studies/{}/series?includefield=SeriesDate", a()),
    )
    .await;
    assert_eq!(val(&rows[0], "00080021"), "20260106");

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let left: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM instances WHERE scan_version = 0")
            .fetch_one(&pool)
            .await
            .unwrap();
        let data: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM instance_data")
            .fetch_one(&pool)
            .await
            .unwrap();
        if left == 0 && data == 5 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "backfill: {left} left, {data} rows"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    srv.stop().await;
}

/// `includefield=all` at instance level reads every instance's metadata.
/// The body is streamed a chunk of rows at a time, not built whole.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn includefield_all_instances_streams_in_chunks() {
    let srv = TestServer::start(|_| {}).await;
    let comment = |i: u32| format!("comment {i}");
    let files: Vec<Vec<u8>> = (1..=130)
        .map(|i| dicom_with(1, 1, 1, i, &[(tags::IMAGE_COMMENTS, VR::LT, &comment(i))]))
        .collect();
    srv.stow(&files).await;
    let path = "/dicom-web/instances?includefield=all";
    let (head, chunks) = http11_chunks(srv.addr(), path, &[]).await;
    assert!(head.starts_with("http/1.1 200"), "{head}");
    assert!(head.contains("transfer-encoding: chunked"), "{head}");
    // 130 rows, 64 per chunk.
    assert!(chunks.len() >= 3, "{} chunks", chunks.len());
    let rows: Vec<Value> = serde_json::from_slice(&chunks.concat()).unwrap();
    assert_eq!(rows.len(), 130);
    // Each row carries its own instance's metadata.
    for row in &rows {
        let n = val(row, "00200013").as_i64().unwrap() as u32;
        assert_eq!(val(row, "00204000"), comment(n).as_str());
        assert_eq!(val(row, "00280004"), "MONOCHROME2");
    }
    // The same answer as a whole (HTTP/1.0) body.
    assert_eq!(qido(&srv, path).await, rows);
    srv.stop().await;
}
