CREATE TABLE uploads (
    id              INTEGER PRIMARY KEY,
    kind            TEXT NOT NULL,
    name            TEXT NOT NULL,
    remote          TEXT,
    started_at      TEXT NOT NULL,
    finished_at     TEXT,
    dicom_stored    INTEGER NOT NULL DEFAULT 0,
    dicom_duplicate INTEGER NOT NULL DEFAULT 0,
    nifti_stored    INTEGER NOT NULL DEFAULT 0,
    nifti_duplicate INTEGER NOT NULL DEFAULT 0,
    skipped         INTEGER NOT NULL DEFAULT 0,
    failed          INTEGER NOT NULL DEFAULT 0,
    status          TEXT NOT NULL
);

CREATE TABLE nifti_volumes (
    id            INTEGER PRIMARY KEY,
    blob_sha256   TEXT NOT NULL UNIQUE,
    size_bytes    INTEGER NOT NULL,
    compressed    INTEGER NOT NULL,
    nifti_version INTEGER NOT NULL,
    original_name TEXT NOT NULL,
    dims          TEXT NOT NULL,
    pixdim        TEXT NOT NULL,
    datatype      INTEGER NOT NULL,
    descrip       TEXT NOT NULL,
    patient_fk    INTEGER REFERENCES patients(id),
    study_fk      INTEGER REFERENCES studies(id),
    upload_fk     INTEGER NOT NULL REFERENCES uploads(id),
    received_at   TEXT NOT NULL
);

CREATE INDEX nifti_volumes_upload_fk ON nifti_volumes(upload_fk);
CREATE INDEX nifti_volumes_patient_fk ON nifti_volumes(patient_fk);
