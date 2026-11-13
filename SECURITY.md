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

See [docs/CONFORMANCE.md](docs/CONFORMANCE.md) for the defaults and limits.
