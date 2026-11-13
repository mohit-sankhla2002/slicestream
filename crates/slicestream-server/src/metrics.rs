//! Background metrics sampler and 300-entry ring buffer.
//!
//! The sampler runs once per second, reading process numbers from `sysinfo`
//! and `/proc`, and every 5 s refreshing the storage block (index counts and
//! file sizes). Both endpoints only read the buffer.

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::Serialize;
use slicestream_store::Ingestor;
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};
use tokio_util::sync::CancellationToken;

const HISTORY_LEN: usize = 300;
const SAMPLE_EVERY: Duration = Duration::from_secs(1);
const STORAGE_EVERY: Duration = Duration::from_secs(5);

/// Fixed-capacity buffer that drops the oldest item when full.
pub(crate) struct Ring<T> {
    cap: usize,
    items: VecDeque<T>,
}

impl<T> Ring<T> {
    pub(crate) fn new(cap: usize) -> Self {
        Self {
            cap,
            items: VecDeque::with_capacity(cap),
        }
    }

    pub(crate) fn push(&mut self, item: T) {
        if self.items.len() == self.cap {
            self.items.pop_front();
        }
        self.items.push_back(item);
    }

    pub(crate) fn last(&self) -> Option<&T> {
        self.items.back()
    }

    pub(crate) fn to_vec(&self) -> Vec<T>
    where
        T: Clone,
    {
        self.items.iter().cloned().collect()
    }
}

/// Live gauges updated by the HTTP middleware and the WebSocket handler.
#[derive(Default)]
pub struct Gauges {
    pub ws_streams: AtomicUsize,
    pub http_in_flight: AtomicUsize,
}

#[derive(Clone, Copy, Debug)]
pub enum Gauge {
    WsStreams,
    HttpInFlight,
}

/// Increments a gauge on creation and decrements it on drop.
pub struct GaugeGuard {
    gauges: Arc<Gauges>,
    which: Gauge,
}

impl GaugeGuard {
    pub fn enter(gauges: &Arc<Gauges>, which: Gauge) -> GaugeGuard {
        let g = GaugeGuard {
            gauges: Arc::clone(gauges),
            which,
        };
        g.counter().fetch_add(1, Ordering::Relaxed);
        g
    }

    fn counter(&self) -> &AtomicUsize {
        match self.which {
            Gauge::WsStreams => &self.gauges.ws_streams,
            Gauge::HttpInFlight => &self.gauges.http_in_flight,
        }
    }
}

impl Drop for GaugeGuard {
    fn drop(&mut self) {
        self.counter().fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct ProcessSample {
    pub cpu_percent: f64,
    pub cores: usize,
    pub rss_bytes: u64,
    pub virtual_bytes: u64,
    pub threads: Option<u64>,
    pub open_fds: Option<u64>,
    pub disk_read_bytes_per_sec: Option<f64>,
    pub disk_write_bytes_per_sec: Option<f64>,
    pub disk_read_total_bytes: Option<u64>,
    pub disk_write_total_bytes: Option<u64>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct StorageSample {
    /// Only present in [`Snapshot`]; history samples carry `None`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub root: Option<String>,
    pub objects_bytes: Option<i64>,
    pub objects_count: Option<i64>,
    pub index_bytes: Option<u64>,
    pub tmp_bytes: Option<u64>,
    pub volume_free_bytes: Option<u64>,
    pub studies: Option<i64>,
    pub series: Option<i64>,
    pub instances: Option<i64>,
}

#[derive(Clone, Debug, Serialize)]
pub struct LatencySample {
    pub p50: Option<f64>,
    pub p95: Option<f64>,
}

#[derive(Clone, Debug, Serialize)]
pub struct IngestSample {
    pub stored: u64,
    pub duplicate: u64,
    pub failed: u64,
    pub per_sec: f64,
    pub latency_ms: LatencySample,
    pub bytes_per_sec: f64,
}

#[derive(Clone, Debug, Serialize)]
pub struct ConnectionsSample {
    pub dimse_active: Option<usize>,
    pub dimse_max: Option<usize>,
    pub ws_streams: usize,
    pub http_in_flight: usize,
}

/// One history entry: the `/api/metrics` shape minus the static fields.
#[derive(Clone, Debug, Serialize)]
pub struct Sample {
    pub sampled_at_ms: u64,
    pub uptime_secs: u64,
    pub process: ProcessSample,
    pub storage: StorageSample,
    pub ingest: IngestSample,
    pub connections: ConnectionsSample,
}

/// The `/api/metrics` body: a [`Sample`] plus static fields.
#[derive(Clone, Debug, Serialize)]
pub struct Snapshot {
    pub pid: u32,
    pub version: &'static str,
    pub http_addr: String,
    pub dimse_addr: Option<String>,
    #[serde(flatten)]
    pub sample: Sample,
}

pub struct MetricsSources {
    pub ingestor: Arc<Ingestor>,
    pub gauges: Arc<Gauges>,
    pub dimse_active: Option<Arc<AtomicUsize>>,
    pub dimse_max: Option<usize>,
    pub http_addr: SocketAddr,
    pub dimse_addr: Option<SocketAddr>,
}

/// Shared read side of the sampler.
pub struct Metrics {
    ring: Mutex<Ring<Sample>>,
    static_info: StaticInfo,
}

struct StaticInfo {
    pid: u32,
    http_addr: String,
    dimse_addr: Option<String>,
    root: String,
}

impl Metrics {
    pub fn latest(&self) -> Option<Snapshot> {
        let mut sample = self.ring.lock().ok()?.last()?.clone();
        sample.storage.root = Some(self.static_info.root.clone());
        Some(Snapshot {
            pid: self.static_info.pid,
            version: env!("CARGO_PKG_VERSION"),
            http_addr: self.static_info.http_addr.clone(),
            dimse_addr: self.static_info.dimse_addr.clone(),
            sample,
        })
    }

    /// Oldest first, at most 300 entries.
    pub fn history(&self) -> Vec<Sample> {
        self.ring.lock().map(|r| r.to_vec()).unwrap_or_default()
    }
}

/// Per-second rate from two counter readings; a counter reset yields 0.
fn rate(prev: u64, now: u64, secs: f64) -> f64 {
    if secs <= 0.0 {
        return 0.0;
    }
    now.saturating_sub(prev) as f64 / secs
}

fn count_dir(path: &str) -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        std::fs::read_dir(path).ok().map(|d| d.count() as u64)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = path;
        None
    }
}

fn file_size(path: &Path) -> u64 {
    std::fs::metadata(path).map(|m| m.len()).unwrap_or(0)
}

/// Blocking: file sizes and free space under the storage root.
fn read_files(root: &Path) -> (Option<u64>, Option<u64>, Option<u64>) {
    let index = ["index.sqlite", "index.sqlite-wal", "index.sqlite-shm"]
        .iter()
        .map(|n| file_size(&root.join(n)))
        .sum();
    let tmp = match std::fs::read_dir(root.join("tmp")) {
        Ok(rd) => Some(
            rd.filter_map(Result::ok)
                .filter_map(|e| e.metadata().ok())
                .filter(|m| m.is_file())
                .map(|m| m.len())
                .sum(),
        ),
        Err(e) => {
            tracing::debug!(error = %e, "metrics: reading tmp dir failed");
            None
        }
    };
    let free = match fs4::available_space(root) {
        Ok(v) => Some(v),
        Err(e) => {
            tracing::debug!(error = %e, "metrics: available_space failed");
            None
        }
    };
    (Some(index), tmp, free)
}

async fn read_storage(ing: &Ingestor) -> StorageSample {
    let mut s = StorageSample::default();
    match ing.index().storage_counts().await {
        Ok(c) => {
            s.objects_bytes = Some(c.objects_bytes);
            s.objects_count = Some(c.objects_count);
            s.studies = Some(c.studies);
            s.series = Some(c.series);
            s.instances = Some(c.instances);
        }
        Err(e) => tracing::debug!(error = %e, "metrics: storage_counts failed"),
    }
    let root = ing.config().root.clone();
    match tokio::task::spawn_blocking(move || read_files(&root)).await {
        Ok((index, tmp, free)) => {
            s.index_bytes = index;
            s.tmp_bytes = tmp;
            s.volume_free_bytes = free;
        }
        Err(e) => tracing::debug!(error = %e, "metrics: file stats task failed"),
    }
    s
}

struct ProcRead {
    cpu_percent: f64,
    rss: u64,
    virt: u64,
    threads: Option<u64>,
    fds: Option<u64>,
    disk: Option<(u64, u64)>,
}

/// Blocking: refresh our own process and read procfs counts.
fn read_process(sys: &mut System, pid: Pid) -> ProcRead {
    sys.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        true,
        ProcessRefreshKind::nothing()
            .with_cpu()
            .with_memory()
            .with_disk_usage(),
    );
    let (cpu, rss, virt, disk) = match sys.process(pid) {
        Some(p) => {
            let d = p.disk_usage();
            (
                f64::from(p.cpu_usage()),
                p.memory(),
                p.virtual_memory(),
                Some((d.total_read_bytes, d.total_written_bytes)),
            )
        }
        None => (0.0, 0, 0, None),
    };
    ProcRead {
        cpu_percent: cpu,
        rss,
        virt,
        threads: count_dir("/proc/self/task"),
        fds: count_dir("/proc/self/fd"),
        disk,
    }
}

struct Prev {
    at: Instant,
    ingest_total: u64,
    ingest_bytes: u64,
    disk: Option<(u64, u64)>,
}

/// Starts the sampler: one sample immediately, then one per second until
/// `shutdown` is cancelled.
pub fn spawn_sampler(sources: MetricsSources, shutdown: CancellationToken) -> Arc<Metrics> {
    let metrics = Arc::new(Metrics {
        ring: Mutex::new(Ring::new(HISTORY_LEN)),
        static_info: StaticInfo {
            pid: std::process::id(),
            http_addr: sources.http_addr.to_string(),
            dimse_addr: sources.dimse_addr.map(|a| a.to_string()),
            root: sources.ingestor.config().root.display().to_string(),
        },
    });
    let m = Arc::clone(&metrics);
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(SAMPLE_EVERY);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let started = Instant::now();
        let pid = Pid::from_u32(std::process::id());
        let cores = std::thread::available_parallelism().map_or(1, |n| n.get());
        let mut sys = Some(System::new());
        let mut prev: Option<Prev> = None;
        let mut storage: Option<(Instant, StorageSample)> = None;
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = tick.tick() => {}
            }
            let now = Instant::now();
            let Some(mut s) = sys.take() else { break };
            let (s, pr) = match tokio::task::spawn_blocking(move || {
                let r = read_process(&mut s, pid);
                (s, r)
            })
            .await
            {
                Ok(v) => v,
                Err(e) => {
                    tracing::debug!(error = %e, "metrics: process read failed");
                    break;
                }
            };
            sys = Some(s);

            if storage
                .as_ref()
                .is_none_or(|(at, _)| now.duration_since(*at) >= STORAGE_EVERY)
            {
                storage = Some((now, read_storage(&sources.ingestor).await));
            }
            let storage_sample = storage.as_ref().map(|(_, s)| s.clone()).unwrap_or_default();

            let st = sources.ingestor.stats();
            let total = st.stored + st.duplicate + st.failed;
            let secs = prev
                .as_ref()
                .map_or(0.0, |p| now.duration_since(p.at).as_secs_f64());
            let (per_sec, bytes_per_sec) = match &prev {
                Some(p) => (
                    rate(p.ingest_total, total, secs),
                    rate(p.ingest_bytes, st.bytes, secs),
                ),
                None => (0.0, 0.0),
            };
            let (dr, dw) = match (&prev, pr.disk) {
                (
                    Some(Prev {
                        disk: Some((pr_, pw)),
                        ..
                    }),
                    Some((r, w)),
                ) => (Some(rate(*pr_, r, secs)), Some(rate(*pw, w, secs))),
                (None, Some(_)) => (Some(0.0), Some(0.0)),
                _ => (None, None),
            };
            prev = Some(Prev {
                at: now,
                ingest_total: total,
                ingest_bytes: st.bytes,
                disk: pr.disk,
            });

            let sample = Sample {
                sampled_at_ms: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_or(0, |d| d.as_millis() as u64),
                uptime_secs: started.elapsed().as_secs(),
                process: ProcessSample {
                    cpu_percent: pr.cpu_percent,
                    cores,
                    rss_bytes: pr.rss,
                    virtual_bytes: pr.virt,
                    threads: pr.threads,
                    open_fds: pr.fds,
                    disk_read_bytes_per_sec: dr,
                    disk_write_bytes_per_sec: dw,
                    disk_read_total_bytes: pr.disk.map(|d| d.0),
                    disk_write_total_bytes: pr.disk.map(|d| d.1),
                },
                storage: storage_sample,
                ingest: IngestSample {
                    stored: st.stored,
                    duplicate: st.duplicate,
                    failed: st.failed,
                    per_sec,
                    latency_ms: LatencySample {
                        p50: st.latency_p50_ms,
                        p95: st.latency_p95_ms,
                    },
                    bytes_per_sec,
                },
                connections: ConnectionsSample {
                    dimse_active: sources
                        .dimse_active
                        .as_ref()
                        .map(|a| a.load(Ordering::Relaxed)),
                    dimse_max: sources.dimse_max,
                    ws_streams: sources.gauges.ws_streams.load(Ordering::Relaxed),
                    http_in_flight: sources.gauges.http_in_flight.load(Ordering::Relaxed),
                },
            };
            if let Ok(mut ring) = m.ring.lock() {
                ring.push(sample);
            }
        }
    });
    metrics
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ring_keeps_last_cap_items() {
        let mut r = Ring::new(3);
        for i in 1..=5 {
            r.push(i);
        }
        assert_eq!(r.to_vec(), vec![3, 4, 5]);
        assert_eq!(r.last(), Some(&5));
    }

    #[test]
    fn gauge_guard_decrements_on_drop() {
        let g = Arc::new(Gauges::default());
        let a = GaugeGuard::enter(&g, Gauge::WsStreams);
        let _b = GaugeGuard::enter(&g, Gauge::WsStreams);
        assert_eq!(g.ws_streams.load(Ordering::Relaxed), 2);
        drop(a);
        assert_eq!(g.ws_streams.load(Ordering::Relaxed), 1);
        let h = GaugeGuard::enter(&g, Gauge::HttpInFlight);
        assert_eq!(g.http_in_flight.load(Ordering::Relaxed), 1);
        drop(h);
        assert_eq!(g.http_in_flight.load(Ordering::Relaxed), 0);
    }

    fn sample() -> Sample {
        Sample {
            sampled_at_ms: 1,
            uptime_secs: 2,
            process: ProcessSample {
                cpu_percent: 1.5,
                cores: 4,
                rss_bytes: 10,
                virtual_bytes: 20,
                threads: None,
                open_fds: Some(3),
                disk_read_bytes_per_sec: None,
                disk_write_bytes_per_sec: None,
                disk_read_total_bytes: None,
                disk_write_total_bytes: None,
            },
            storage: StorageSample::default(),
            ingest: IngestSample {
                stored: 1,
                duplicate: 0,
                failed: 0,
                per_sec: 0.0,
                latency_ms: LatencySample {
                    p50: Some(1.0),
                    p95: None,
                },
                bytes_per_sec: 0.0,
            },
            connections: ConnectionsSample {
                dimse_active: None,
                dimse_max: None,
                ws_streams: 0,
                http_in_flight: 0,
            },
        }
    }

    #[test]
    fn snapshot_json_shape() {
        let mut s = sample();
        s.storage.root = Some("storage".into());
        let snap = Snapshot {
            pid: 7,
            version: "0.1.0",
            http_addr: "127.0.0.1:8080".into(),
            dimse_addr: None,
            sample: s,
        };
        let v = serde_json::to_value(&snap).unwrap();
        let mut keys: Vec<&str> = v.as_object().unwrap().keys().map(|k| k.as_str()).collect();
        keys.sort();
        let mut want = vec![
            "sampled_at_ms",
            "pid",
            "version",
            "uptime_secs",
            "http_addr",
            "dimse_addr",
            "process",
            "storage",
            "ingest",
            "connections",
        ];
        want.sort();
        assert_eq!(keys, want);
        assert!(v["process"]["threads"].is_null());
        assert_eq!(v["process"]["open_fds"], 3);
        assert_eq!(v["ingest"]["latency_ms"]["p50"], 1.0);
        assert!(v["ingest"]["latency_ms"]["p95"].is_null());
        assert_eq!(v["storage"]["root"], "storage");
        assert!(v["dimse_addr"].is_null());

        let h = serde_json::to_value(sample()).unwrap();
        assert!(h.get("pid").is_none());
        assert!(h["storage"].get("root").is_none());
        assert!(h["storage"]["objects_bytes"].is_null());
    }

    #[test]
    fn rate_from_deltas() {
        assert_eq!(rate(100, 1100, 2.0), 500.0);
        assert_eq!(rate(5, 3, 1.0), 0.0);
        assert_eq!(rate(1, 2, 0.0), 0.0);
    }

    #[tokio::test]
    async fn sampler_produces_a_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let ing = Ingestor::open(slicestream_store::StorageConfig {
            root: dir.path().to_path_buf(),
            ..Default::default()
        })
        .await
        .unwrap();
        let shutdown = CancellationToken::new();
        let m = spawn_sampler(
            MetricsSources {
                ingestor: ing,
                gauges: Arc::new(Gauges::default()),
                dimse_active: None,
                dimse_max: None,
                http_addr: "127.0.0.1:8080".parse().unwrap(),
                dimse_addr: None,
            },
            shutdown.clone(),
        );
        let mut snap = None;
        for _ in 0..50 {
            snap = m.latest();
            if snap.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        shutdown.cancel();
        let snap = snap.expect("first sample");
        assert_eq!(snap.sample.storage.instances, Some(0));
        assert!(snap.sample.storage.volume_free_bytes.is_some());
        assert!(snap.sample.process.rss_bytes > 0);
        assert_eq!(m.history().len(), 1);
    }
}
