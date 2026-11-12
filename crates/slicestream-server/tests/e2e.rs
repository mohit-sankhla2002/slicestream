//! End-to-end tests: the full server (HTTP + DIMSE) on ephemeral ports.

#[allow(dead_code)]
#[path = "../../slicestream-core/examples/make_phantom.rs"]
mod phantom;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use dicom_core::{DataElement, VR, dicom_value};
use dicom_dictionary_std::{tags, uids};
use dicom_object::{InMemDicomObject, open_file};
use dicom_transfer_syntax_registry::entries::{
    EXPLICIT_VR_LITTLE_ENDIAN, IMPLICIT_VR_LITTLE_ENDIAN,
};
use dicom_ul::ClientAssociationOptions;
use dicom_ul::Pdu;
use dicom_ul::pdu::{PDataValue, PDataValueType};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use slicestream_core::protocol::decode_frame;
use slicestream_server::config::Config;
use slicestream_server::{Running, import_dir, start};
use slicestream_store::{Ingestor, StorageConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

const SERIES: &str = "2.25.1000000000000000000000000000001";
const BOUNDARY: &str = "XXe2eXX";

struct Server {
    running: Running,
    shutdown: CancellationToken,
    phantom: Vec<PathBuf>,
    _dir: tempfile::TempDir,
}

impl Server {
    async fn start(slices: usize) -> Server {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("phantom");
        phantom::write_phantom(&data, slices, 32).unwrap();
        let mut files: Vec<PathBuf> = std::fs::read_dir(&data)
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect();
        files.sort();

        let mut config = Config::default();
        config.storage.root = dir.path().join("store");
        config.http.addr = "127.0.0.1:0".parse().unwrap();
        config.http.web_dir = dir.path().join("web");
        config.dimse.addr = "127.0.0.1:0".parse().unwrap();

        let shutdown = CancellationToken::new();
        let running = start(config, shutdown.clone()).await.unwrap();
        Server {
            running,
            shutdown,
            phantom: files,
            _dir: dir,
        }
    }

    async fn stop(self) {
        self.stop_keep_dir().await;
    }

    /// Stop the server and hand back its temp dir (storage still on disk).
    async fn stop_keep_dir(self) -> tempfile::TempDir {
        self.shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(10), self.running.task)
            .await
            .expect("server stops promptly")
            .unwrap()
            .unwrap();
        self._dir
    }
}

/// Minimal HTTP/1.0 client: one request per connection, so responses are
/// never chunked and end at EOF.
async fn http(addr: SocketAddr, method: &str, path: &str, ct: &str, body: &[u8]) -> (u16, Value) {
    let mut s = TcpStream::connect(addr).await.unwrap();
    let head = format!(
        "{method} {path} HTTP/1.0\r\nHost: {addr}\r\nContent-Type: {ct}\r\nContent-Length: {}\r\n\r\n",
        body.len()
    );
    s.write_all(head.as_bytes()).await.unwrap();
    s.write_all(body).await.unwrap();
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).await.unwrap();
    let split = buf
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("response head");
    let head = std::str::from_utf8(&buf[..split]).unwrap();
    let status: u16 = head.split(' ').nth(1).unwrap().parse().unwrap();
    let json = serde_json::from_slice(&buf[split + 4..]).unwrap_or(Value::Null);
    (status, json)
}

async fn list_series(addr: SocketAddr) -> Vec<Value> {
    let (status, json) = http(addr, "GET", "/api/series", "text/plain", b"").await;
    assert_eq!(status, 200);
    json.as_array().unwrap().clone()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stow_then_stream() {
    let srv = Server::start(5).await;
    let addr = srv.running.http_addr;

    let mut body = Vec::new();
    for f in &srv.phantom {
        body.extend_from_slice(
            format!("--{BOUNDARY}\r\nContent-Type: application/dicom\r\n\r\n").as_bytes(),
        );
        body.extend_from_slice(&std::fs::read(f).unwrap());
        body.extend_from_slice(b"\r\n");
    }
    body.extend_from_slice(format!("--{BOUNDARY}--\r\n").as_bytes());
    let ct = format!("multipart/related; type=\"application/dicom\"; boundary={BOUNDARY}");
    let (status, json) = http(addr, "POST", "/dicom-web/studies", &ct, &body).await;
    assert_eq!(status, 200, "{json}");

    let list = list_series(addr).await;
    assert_eq!(list.len(), 1, "{list:?}");
    assert_eq!(list[0]["uid"], SERIES);
    assert_eq!(list[0]["sliceCount"], 5);

    let url = format!("ws://{addr}/api/series/{SERIES}/stream");
    let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
    let hello = ws.next().await.unwrap().unwrap();
    let hello: Value = serde_json::from_str(hello.to_text().unwrap()).unwrap();
    assert_eq!(hello["type"], "series");
    ws.send(Message::Text(
        r#"{"type":"cursor","index":2,"velocity":0}"#.into(),
    ))
    .await
    .unwrap();
    let first = loop {
        match ws.next().await.unwrap().unwrap() {
            Message::Binary(b) => break b,
            Message::Text(_) | Message::Ping(_) | Message::Pong(_) => continue,
            other => panic!("unexpected message {other:?}"),
        }
    };
    let (header, payload) = decode_frame(&first).expect("valid frame");
    assert_eq!(header.index, 2);
    assert_eq!(payload.len(), 32 * 32 * 2);
    drop(ws);

    srv.stop().await;
}

// --- minimal C-STORE SCU -------------------------------------------------

fn command(elems: Vec<DataElement<InMemDicomObject>>) -> Vec<u8> {
    let mut buf = Vec::new();
    InMemDicomObject::command_from_element_iter(elems)
        .write_dataset_with_ts(&mut buf, &IMPLICIT_VR_LITTLE_ENDIAN.erased())
        .unwrap();
    buf
}

fn store_rq(message_id: u16, sop_class: &str, sop_instance: &str) -> Vec<u8> {
    command(vec![
        DataElement::new(
            tags::AFFECTED_SOP_CLASS_UID,
            VR::UI,
            dicom_value!(Str, sop_class),
        ),
        DataElement::new(tags::COMMAND_FIELD, VR::US, dicom_value!(U16, [0x0001])),
        DataElement::new(tags::MESSAGE_ID, VR::US, dicom_value!(U16, [message_id])),
        DataElement::new(tags::PRIORITY, VR::US, dicom_value!(U16, [0])),
        DataElement::new(
            tags::COMMAND_DATA_SET_TYPE,
            VR::US,
            dicom_value!(U16, [0x0000]),
        ),
        DataElement::new(
            tags::AFFECTED_SOP_INSTANCE_UID,
            VR::UI,
            dicom_value!(Str, sop_instance),
        ),
    ])
}

fn pdv(pc_id: u8, value_type: PDataValueType, is_last: bool, data: Vec<u8>) -> Pdu {
    Pdu::PData {
        data: vec![PDataValue {
            presentation_context_id: pc_id,
            value_type,
            is_last,
            data,
        }],
    }
}

async fn c_store_files(addr: SocketAddr, files: &[PathBuf]) {
    let mut scu = ClientAssociationOptions::new()
        .calling_ae_title("E2ESCU")
        .called_ae_title("SLICESTREAM")
        .with_presentation_context(
            uids::CT_IMAGE_STORAGE,
            vec![EXPLICIT_VR_LITTLE_ENDIAN.uid()],
        )
        .read_timeout(Duration::from_secs(10))
        .connection_timeout(Duration::from_secs(10))
        .establish_async(addr)
        .await
        .expect("association accepted");
    let pc_id = scu.presentation_contexts()[0].id;
    let max = (scu.acceptor_max_pdu_length() as usize - 6).min(16 * 1024);

    for (i, path) in files.iter().enumerate() {
        let obj = open_file(path).unwrap();
        let sop_class = obj
            .meta()
            .media_storage_sop_class_uid()
            .trim_end_matches('\0');
        let sop_instance = obj
            .meta()
            .media_storage_sop_instance_uid()
            .trim_end_matches('\0');
        let mut dataset = Vec::new();
        obj.write_dataset_with_ts(&mut dataset, &EXPLICIT_VR_LITTLE_ENDIAN.erased())
            .unwrap();

        let cmd = store_rq(i as u16 + 1, sop_class, sop_instance);
        scu.send(&pdv(pc_id, PDataValueType::Command, true, cmd))
            .await
            .unwrap();
        let chunks: Vec<&[u8]> = dataset.chunks(max).collect();
        let n = chunks.len();
        for (j, c) in chunks.into_iter().enumerate() {
            scu.send(&pdv(pc_id, PDataValueType::Data, j + 1 == n, c.to_vec()))
                .await
                .unwrap();
        }

        let mut rsp = Vec::new();
        let status = 'outer: loop {
            match scu.receive().await.unwrap() {
                Pdu::PData { data } => {
                    for v in data {
                        rsp.extend_from_slice(&v.data);
                        if v.is_last {
                            let obj = InMemDicomObject::read_dataset_with_ts(
                                &rsp[..],
                                &IMPLICIT_VR_LITTLE_ENDIAN.erased(),
                            )
                            .unwrap();
                            break 'outer obj
                                .element(tags::STATUS)
                                .unwrap()
                                .to_int::<u16>()
                                .unwrap();
                        }
                    }
                }
                other => panic!("unexpected PDU {other:?}"),
            }
        };
        assert_eq!(status, 0x0000, "C-STORE of {}", path.display());
    }
    scu.release().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn c_store_then_listed() {
    let srv = Server::start(3).await;
    let http_addr = srv.running.http_addr;
    let dimse_addr = srv.running.dimse_addr.expect("DIMSE enabled by default");

    assert!(list_series(http_addr).await.is_empty());

    // One slice first, so the series gets built and cached...
    c_store_files(dimse_addr, &srv.phantom[..1]).await;
    let list = list_series(http_addr).await;
    assert_eq!(list.len(), 1);
    assert_eq!(list[0]["sliceCount"], 1);

    // ...then the rest must show up without a restart.
    c_store_files(dimse_addr, &srv.phantom[1..]).await;
    let list = list_series(http_addr).await;
    assert_eq!(list.len(), 1);
    assert_eq!(list[0]["uid"], SERIES);
    assert_eq!(list[0]["sliceCount"], 3);
    let (status, one) = http(
        http_addr,
        "GET",
        &format!("/api/series/{SERIES}"),
        "text/plain",
        b"",
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(one["sliceCount"], 3);

    srv.stop().await;
}

#[tokio::test]
async fn import_command_counts() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("in");
    phantom::write_phantom(&data, 4, 16).unwrap();
    std::fs::write(data.join("notes.txt"), b"not a DICOM file").unwrap();
    let ingestor = Ingestor::open(StorageConfig {
        root: dir.path().join("store"),
        ..StorageConfig::default()
    })
    .await
    .unwrap();

    let report = import_dir(&ingestor, &data, false).await;
    assert_eq!(report.stored, 4);
    assert_eq!(report.skipped, 1);
    assert_eq!(report.duplicate, 0);
    assert_eq!(report.failed, 0);

    // Importing again: every DICOM file is a duplicate.
    let again = import_dir(&ingestor, &data, false).await;
    assert_eq!(again.stored, 0);
    assert_eq!(again.duplicate, 4);
    assert_eq!(again.skipped, 1);
}

#[tokio::test]
async fn import_recursive_descends() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("in");
    phantom::write_phantom(&data.join("sub"), 2, 16).unwrap();
    let ingestor = Ingestor::open(StorageConfig {
        root: dir.path().join("store"),
        ..StorageConfig::default()
    })
    .await
    .unwrap();

    assert_eq!(import_dir(&ingestor, &data, false).await.stored, 0);
    assert_eq!(import_dir(&ingestor, &data, true).await.stored, 2);
}

fn write(dir: &Path, name: &str, text: &str) -> PathBuf {
    let p = dir.join(name);
    std::fs::write(&p, text).unwrap();
    p
}

#[test]
fn config_rejects_unknown_key() {
    let dir = tempfile::tempdir().unwrap();
    let p = write(dir.path(), "bad.toml", "[storage]\nrot = \"x\"\n");
    let err = Config::load(Some(&p)).unwrap_err();
    assert!(format!("{err:#}").contains("rot"), "{err:#}");
}

#[test]
fn config_defaults_without_file() {
    assert_eq!(Config::load(None).unwrap(), Config::default());
}

#[test]
fn example_config_matches_defaults() {
    let p = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../slicestream.example.toml");
    assert_eq!(Config::load(Some(&p)).unwrap(), Config::default());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn catalog_get_sees_new_slices_after_stored_event() {
    use slicestream_server::SeriesCatalog;

    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("in");
    phantom::write_phantom(&data, 2, 16).unwrap();
    let ingestor = Ingestor::open(StorageConfig {
        root: dir.path().join("store"),
        ..StorageConfig::default()
    })
    .await
    .unwrap();
    let catalog = SeriesCatalog::new(std::sync::Arc::clone(&ingestor));
    catalog.spawn_invalidator();

    assert!(catalog.get(SERIES).await.unwrap().is_none());
    ingestor
        .import_file(&data.join("IM00001.dcm"))
        .await
        .unwrap();
    // `get` alone (no `list` refresh) must pick up each new slice.
    let mut len = 0;
    for _ in 0..200 {
        len = catalog.get(SERIES).await.unwrap().map_or(0, |s| s.len());
        if len == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(len, 1);
    ingestor
        .import_file(&data.join("IM00002.dcm"))
        .await
        .unwrap();
    for _ in 0..200 {
        len = catalog.get(SERIES).await.unwrap().map_or(0, |s| s.len());
        if len == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(len, 2);
}

#[cfg(unix)]
#[tokio::test]
async fn import_counts_unreadable_dir_as_failed() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("in");
    phantom::write_phantom(&data, 2, 16).unwrap();
    let locked = data.join("locked");
    phantom::write_phantom(&locked, 1, 16).unwrap();
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o000)).unwrap();
    // Root (or CAP_DAC_OVERRIDE) can read it anyway; nothing to test then.
    let privileged = std::fs::read_dir(&locked).is_ok();

    let ingestor = Ingestor::open(StorageConfig {
        root: dir.path().join("store"),
        ..StorageConfig::default()
    })
    .await
    .unwrap();
    let report = import_dir(&ingestor, &data, true).await;
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755)).unwrap();
    if privileged {
        eprintln!("skipping: running with privileges that bypass mode 000");
        return;
    }
    assert_eq!(report.stored, 2, "{report:?}");
    assert_eq!(report.failed, 1, "{report:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn running_server_owns_its_storage_root() {
    let srv = Server::start(1).await;
    let root = srv._dir.path().join("store");
    let err = Ingestor::open(StorageConfig {
        root: root.clone(),
        ..StorageConfig::default()
    })
    .await
    .unwrap_err();
    assert!(
        matches!(err, slicestream_store::IngestError::Locked { .. }),
        "{err:?}"
    );
    let dir = srv.stop_keep_dir().await;
    // A stopped server has released the lock.
    Ingestor::open(StorageConfig {
        root: dir.path().join("store"),
        ..StorageConfig::default()
    })
    .await
    .expect("lock released after shutdown");
}
