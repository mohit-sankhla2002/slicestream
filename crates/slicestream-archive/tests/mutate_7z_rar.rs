//! Corrupted 7z and RAR inputs: every truncation and many random byte
//! flips. The walk must never panic (not even a panic caught internally),
//! must not abort for reasons other than the limits, and must leave
//! `tmp_dir` empty.

mod support;

use std::sync::atomic::{AtomicUsize, Ordering};

use slicestream_archive::{Limits, WalkError};
use support::*;

static PANICS: AtomicUsize = AtomicUsize::new(0);

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

fn check(bytes: &[u8], name: &str) {
    if let Some(p) = std::env::var_os("MUTATE_TRACE") {
        let _ = std::fs::write(p, bytes);
        eprintln!("case {name} {}", bytes.len());
    }
    let limits = Limits {
        max_expanded_bytes: 64 * 1024 * 1024,
        ..Limits::default()
    };
    let (r, _rec, e) = run(bytes, name, &limits);
    match r {
        Ok(_) => {}
        Err(WalkError::Aborted(m)) => {
            assert!(
                m.starts_with("too many entries") || m.starts_with("expanded size limit"),
                "{m}"
            );
        }
        Err(WalkError::Io(e)) => panic!("i/o error: {e}"),
    }
    assert_tmp_empty(&e.tmp);
}

fn mutate(base: &[u8], name: &str, rounds: usize, seed: u64) {
    // Every cut in the signature/start header, then every few bytes.
    for cut in (0..base.len()).filter(|&c| c < 64 || c % 5 == 0) {
        check(&base[..cut], name);
    }
    // MUTATE_SCALE=<n> runs n times as many random rounds.
    let scale: usize = std::env::var("MUTATE_SCALE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    let mut rng = Rng(seed);
    for _ in 0..rounds * scale {
        let mut b = base.to_vec();
        for _ in 0..1 + rng.next() % 4 {
            let i = (rng.next() % b.len() as u64) as usize;
            b[i] = rng.next() as u8;
        }
        check(&b, name);
    }
}

#[test]
fn corrupted_archives_never_panic() {
    let prev = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        PANICS.fetch_add(1, Ordering::SeqCst);
        prev(info);
    }));

    let a = dicom("A");
    let b = dicom("B");
    let names: Vec<(String, Vec<u8>)> = (0..12)
        .map(|i| (format!("dir/sub/IM{i:04}"), dicom(&format!("{i}"))))
        .collect();
    let refs: Vec<(&str, &[u8])> = names
        .iter()
        .map(|(n, d)| (n.as_str(), d.as_slice()))
        .collect();
    mutate(
        &seven_z(&[("IM1", &a), ("d/", b""), ("IM2", &b)]),
        "x.7z",
        600,
        1,
    );
    mutate(&seven_z(&refs), "h.7z", 300, 2);
    mutate(&seven_z_solid(&[("IM1", &a), ("IM2", &b)]), "s.7z", 600, 3);
    mutate(&seven_z_stored(&[("IM1", &a), ("IM2", &b)]), "c.7z", 600, 4);
    mutate(&seven_z_encrypted(&[("IM1", &a)], false), "e.7z", 300, 5);
    if cfg!(feature = "rar") {
        let inner = zip(&[("IM2", &b)]);
        let r5 = rar5(
            &[
                RarItem::File("IM1", &a),
                RarItem::Dir("d"),
                RarItem::File("d/inner.zip", &inner),
                RarItem::Encrypted("IM9", &a),
            ],
            false,
        );
        mutate(&r5, "x.rar", 800, 6);
        mutate(&rar5(&[RarItem::File("IM1", &a)], true), "s.rar", 300, 7);
    }
    assert_eq!(PANICS.load(Ordering::SeqCst), 0, "a panic occurred");
}

/// Replays one input: `MUTATE_REPLAY=<file> cargo test --test mutate_7z_rar replay -- --ignored`.
#[test]
#[ignore]
fn replay() {
    let p = std::env::var_os("MUTATE_REPLAY").expect("MUTATE_REPLAY");
    let name = std::env::var("MUTATE_NAME").unwrap_or_else(|_| "x.7z".into());
    let t = std::time::Instant::now();
    let limits = Limits {
        max_expanded_bytes: 64 * 1024 * 1024,
        ..Limits::default()
    };
    let (r, rec, _e) = run(&std::fs::read(p).unwrap(), &name, &limits);
    eprintln!(
        "{:?} {:?} {:?} {:?}",
        t.elapsed(),
        r,
        rec.failed,
        rec.entry_failed
    );
}
