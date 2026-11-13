//! Series catalog backed by the storage index.
//!
//! A [`Series`] is built from index rows on first use and cached. Each
//! [`StoredEvent`](slicestream_store::StoredEvent) drops the affected entry, so
//! newly stored slices are streamable at once.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};

use slicestream_core::series::SeriesSummary;
use slicestream_core::{Series, SliceRef};
use slicestream_store::{BlobKind, Ingestor, SeriesRow};
use tokio::sync::RwLock;
use tokio::sync::broadcast::error::RecvError;

struct Entry {
    /// Instance count of the index row this entry was built from.
    instance_count: i64,
    /// `None` when the series has no image frames.
    series: Option<Arc<Series>>,
}

pub struct SeriesCatalog {
    ingestor: Arc<Ingestor>,
    cache: RwLock<HashMap<String, Entry>>,
    /// Bumped on every invalidation. A build that started before an
    /// invalidation is not cached, so it cannot resurrect stale slices.
    generation: AtomicU64,
}

impl SeriesCatalog {
    pub fn new(ingestor: Arc<Ingestor>) -> Arc<Self> {
        Arc::new(SeriesCatalog {
            ingestor,
            cache: RwLock::new(HashMap::new()),
            generation: AtomicU64::new(0),
        })
    }

    /// Summaries of every series with at least one image frame, by UID.
    pub async fn list(&self) -> anyhow::Result<Vec<SeriesSummary>> {
        let rows = self.ingestor.index().list_series().await?;
        let mut out = Vec::with_capacity(rows.len());
        for row in &rows {
            if let Some(series) = self.get_row(row).await? {
                out.push(series.summary());
            }
        }
        out.sort_by(|a, b| a.uid.cmp(&b.uid));
        Ok(out)
    }

    /// The series with this UID, or `None` if it is unknown or has no image frames.
    pub async fn get(&self, series_uid: &str) -> anyhow::Result<Option<Arc<Series>>> {
        if let Some(e) = self.cache.read().await.get(series_uid) {
            return Ok(e.series.clone());
        }
        let rows = self.ingestor.index().list_series().await?;
        match rows.iter().find(|r| r.series_instance_uid == series_uid) {
            Some(row) => self.get_row(row).await,
            None => Ok(None),
        }
    }

    /// Cached series for `row`, rebuilt if missing or if the index has a
    /// different instance count (a backstop in case an invalidation was
    /// missed).
    async fn get_row(&self, row: &SeriesRow) -> anyhow::Result<Option<Arc<Series>>> {
        if let Some(e) = self.cache.read().await.get(&row.series_instance_uid)
            && e.instance_count == row.instance_count
        {
            return Ok(e.series.clone());
        }
        let generation = self.generation.load(Ordering::Acquire);
        let series = self.build(row).await?;
        let mut cache = self.cache.write().await;
        if self.generation.load(Ordering::Acquire) == generation {
            cache.insert(
                row.series_instance_uid.clone(),
                Entry {
                    instance_count: row.instance_count,
                    series: series.clone(),
                },
            );
        }
        Ok(series)
    }

    async fn build(&self, row: &SeriesRow) -> anyhow::Result<Option<Arc<Series>>> {
        let rows = self
            .ingestor
            .index()
            .series_slices(&row.series_instance_uid)
            .await?;
        let blobs = self.ingestor.blobs();
        let mut slices = Vec::new();
        for r in rows {
            let path = blobs.path_of(&r.blob, BlobKind::Dicom);
            for frame in 0..r.number_of_frames {
                slices.push(SliceRef::new(
                    path.clone(),
                    frame,
                    r.frame_info.clone(),
                    r.slice_position,
                    r.instance_number,
                ));
            }
        }
        if slices.is_empty() {
            return Ok(None);
        }
        Ok(Some(Arc::new(Series::from_slices(
            row.series_instance_uid.clone(),
            row.series_description.clone(),
            row.modality.clone(),
            slices,
        ))))
    }

    async fn invalidate(&self, series_uid: Option<&str>) {
        let mut cache = self.cache.write().await;
        self.generation.fetch_add(1, Ordering::AcqRel);
        match series_uid {
            Some(uid) => {
                cache.remove(uid);
            }
            None => cache.clear(),
        }
    }

    /// Drop cache entries as instances are stored. On a lagged receiver the
    /// whole cache is cleared. The task ends when the catalog is dropped or
    /// the ingestor closes its event channel.
    pub fn spawn_invalidator(self: &Arc<Self>) {
        let mut events = self.ingestor.subscribe();
        let weak: Weak<Self> = Arc::downgrade(self);
        tokio::spawn(async move {
            loop {
                let event = events.recv().await;
                let Some(catalog) = weak.upgrade() else { break };
                match event {
                    Ok(e) => catalog.invalidate(Some(&e.series_instance_uid)).await,
                    Err(RecvError::Lagged(n)) => {
                        tracing::warn!(missed = n, "catalog invalidator lagged; clearing cache");
                        catalog.invalidate(None).await;
                    }
                    Err(RecvError::Closed) => break,
                }
            }
        });
    }
}
