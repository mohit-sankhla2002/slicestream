# Security Policy

slicestream handles medical images. Please report vulnerabilities privately.

## Reporting a vulnerability

Use GitHub's private vulnerability reporting:
**Security → Report a vulnerability** on this repository.
Do not open a public issue.

Include:

- affected version or commit
- steps to reproduce
- impact (data loss, unauthorized read or write, denial of service, …)

**Never include real patient data (PHI).** Reproduce with the synthetic
phantom (`cargo run -p slicestream-core --example make_phantom`).

We aim to acknowledge reports within 7 days.

## Supported versions

slicestream is pre-1.0. Security fixes land on `main` only.

## Deployment notes

- slicestream is **not** certified for clinical use.
- DICOM AE titles are not authentication. Restrict DIMSE with `allowed_peers` and a firewall.
- Set `http.stow_token` and keep `http.addr` on localhost unless a reverse proxy adds TLS and authentication.
- `/api/metrics`, `/api/metrics/history` and `/api/console/series` reveal the process id, bind addresses, the storage path and the server load, and list all patient names. They have no token. Set `http.metrics_enabled = false` (this also turns off the console's metrics and series panels), or keep the server on localhost.
- Archive uploads (`POST /api/archives`) are checked against limits in `[archives]`: nesting depth, number of entries, total expanded bytes (zip bombs), and a free-disk guard that refuses or aborts uploads when the volume is nearly full. Archive metadata is capped too (ZIP central directory 256 MiB, 7z header 64 MiB and dictionary 256 MiB, bounded tar headers). Entry names are never used as file paths. Nothing is read into memory whole; data streams through `<root>/tmp/`. Encrypted archives fail. They use the same bearer token as other ingest routes and never send CORS headers.
- `GET /api/archives/{job}` lists patient names and entry paths from the uploaded archive. Like `/api/console/series` it has no token and no CORS. Anyone who can reach the server and learns a job id (128 random bits) can read it; keep the server on localhost or behind authentication. Set `[archives] enabled = false` to remove both routes.
- RAR reading uses the bundled UnRAR C++ library, which parses untrusted input. Build with `--no-default-features` if you do not need RAR.

See [docs/CONFORMANCE.md](docs/CONFORMANCE.md) for the defaults and limits.
