//! Shared budget for one walk and the counting reader that charges it.

use std::cell::{Cell, RefCell};
use std::io::{self, Read};
use std::path::PathBuf;

use crate::walk::Limits;

/// Free space is checked each time this many more bytes have been counted.
const DISK_CHECK_EVERY: u64 = 64 * 1024 * 1024;

/// Limits and running totals for one walk. Shared by reference between the
/// walker and every counting reader, so it uses interior mutability.
pub(crate) struct Budget {
    limits: Limits,
    tmp_dir: PathBuf,
    bytes: Cell<u64>,
    entries: Cell<u64>,
    next_disk_check: Cell<u64>,
    aborted: Cell<bool>,
    reason: RefCell<Option<String>>,
}

impl Budget {
    /// A budget that has already spent `entries` and `bytes`.
    pub(crate) fn new(limits: &Limits, tmp_dir: PathBuf, entries: u64, bytes: u64) -> Self {
        Budget {
            limits: limits.clone(),
            tmp_dir,
            bytes: Cell::new(bytes),
            entries: Cell::new(entries),
            next_disk_check: Cell::new(
                (bytes / DISK_CHECK_EVERY)
                    .saturating_add(1)
                    .saturating_mul(DISK_CHECK_EVERY),
            ),
            aborted: Cell::new(false),
            reason: RefCell::new(None),
        }
    }

    pub(crate) fn limits(&self) -> &Limits {
        &self.limits
    }

    pub(crate) fn bytes(&self) -> u64 {
        self.bytes.get()
    }

    pub(crate) fn entries(&self) -> u64 {
        self.entries.get()
    }

    /// The abort reason, once any limit has been crossed.
    pub(crate) fn aborted(&self) -> Option<String> {
        if self.aborted.get() {
            self.reason.borrow().clone()
        } else {
            None
        }
    }

    /// Record an abort (the first reason wins) and return it.
    fn abort(&self, reason: String) -> String {
        if !self.aborted.get() {
            self.aborted.set(true);
            *self.reason.borrow_mut() = Some(reason);
        }
        self.reason.borrow().clone().unwrap_or_default()
    }

    /// Count one file entry; `Err(reason)` once `max_entries` is crossed.
    pub(crate) fn add_entry(&self) -> Result<(), String> {
        if let Some(r) = self.aborted() {
            return Err(r);
        }
        let n = self.entries.get().saturating_add(1);
        self.entries.set(n);
        if n > self.limits.max_entries {
            return Err(self.abort_entries());
        }
        Ok(())
    }

    /// Abort with the entry-limit reason without counting (an archive that
    /// declares more entries than are left).
    pub(crate) fn abort_entries(&self) -> String {
        self.abort(format!(
            "too many entries (limit {})",
            self.limits.max_entries
        ))
    }

    /// Count expanded bytes; `Err(reason)` once `max_expanded_bytes` is
    /// crossed or free space in `tmp_dir` falls below `min_free_bytes`.
    pub(crate) fn add_bytes(&self, n: u64) -> Result<(), String> {
        if let Some(r) = self.aborted() {
            return Err(r);
        }
        let total = self.bytes.get().saturating_add(n);
        self.bytes.set(total);
        if total > self.limits.max_expanded_bytes {
            return Err(self.abort(format!(
                "expanded size limit reached ({} bytes)",
                self.limits.max_expanded_bytes
            )));
        }
        if total >= self.next_disk_check.get() {
            let next = (total / DISK_CHECK_EVERY)
                .saturating_add(1)
                .saturating_mul(DISK_CHECK_EVERY);
            self.next_disk_check.set(next);
            // An unreadable free-space figure is not a reason to abort.
            if let Ok(free) = fs4::available_space(&self.tmp_dir)
                && free < self.limits.min_free_bytes
            {
                return Err(self.abort("disk almost full".to_string()));
            }
        }
        Ok(())
    }
}

/// Wraps a decoder or entry stream. When `count` is set, every byte read is
/// charged to the budget; after an abort every read fails. Remembers the
/// first read error so the walker can tell a broken input (read side) from a
/// failure of its own (write side / sink).
pub(crate) struct Counted<'b, R> {
    inner: R,
    budget: &'b Budget,
    count: bool,
    read_error: Option<String>,
}

impl<'b, R: Read> Counted<'b, R> {
    pub(crate) fn new(inner: R, budget: &'b Budget, count: bool) -> Self {
        Counted {
            inner,
            budget,
            count,
            read_error: None,
        }
    }

    pub(crate) fn take_error(&mut self) -> Option<String> {
        self.read_error.take()
    }
}

impl<R: Read> Read for Counted<'_, R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if let Some(r) = self.budget.aborted() {
            return Err(io::Error::other(r));
        }
        match self.inner.read(buf) {
            Ok(n) => {
                if self.count
                    && let Err(r) = self.budget.add_bytes(n as u64)
                {
                    return Err(io::Error::other(r));
                }
                Ok(n)
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => Err(e),
            Err(e) => {
                if self.read_error.is_none() {
                    self.read_error = Some(e.to_string());
                }
                Err(e)
            }
        }
    }
}
