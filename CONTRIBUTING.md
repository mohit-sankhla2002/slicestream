# Contributing

Thanks for helping! Before you open a PR:

```sh
cargo fmt --all
cargo clippy --all-targets -- -D warnings
cargo test --all
```

## Rules

- **No patient data.** Never commit, attach or paste real DICOM files or
  screenshots that contain PHI. Use `cargo run -p slicestream-core --example make_phantom`
  or public anonymized datasets.
- Keep `slicestream-core` free of async runtimes and network code. Transport
  belongs in server or binding crates.
- Changes to the wire protocol must update `docs/PROTOCOL.md` and both reference
  implementations (Rust and `web/viewer.js` with `web/lib/viewer-core.js`).
- Every bug fix needs a test.

By contributing, you agree that your work is dual-licensed under MIT and
Apache-2.0, as described in the README.
