CREATE TABLE patients (
    id           INTEGER PRIMARY KEY,
    patient_id   TEXT NOT NULL UNIQUE,
    patient_name TEXT
);

CREATE TABLE studies (
    id                 INTEGER PRIMARY KEY,
    study_instance_uid TEXT NOT NULL UNIQUE,
    patient_fk         INTEGER NOT NULL REFERENCES patients(id),
    study_date         TEXT,
    study_description  TEXT,
    accession_number   TEXT,
    created_at         TEXT NOT NULL,
    updated_at         TEXT NOT NULL
);

CREATE TABLE series (
    id                  INTEGER PRIMARY KEY,
    series_instance_uid TEXT NOT NULL UNIQUE,
    study_fk            INTEGER NOT NULL REFERENCES studies(id),
    modality            TEXT,
    series_number       INTEGER,
    series_description  TEXT,
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL
);

CREATE TABLE instances (
    id               INTEGER PRIMARY KEY,
    sop_instance_uid TEXT NOT NULL UNIQUE,
    series_fk        INTEGER NOT NULL REFERENCES series(id),
    sop_class_uid    TEXT NOT NULL,
    instance_number  INTEGER,
    number_of_frames INTEGER NOT NULL,
    transfer_syntax  TEXT NOT NULL,
    slice_position   REAL,
    frame_info       TEXT,
    blob_sha256      TEXT NOT NULL,
    size_bytes       INTEGER NOT NULL,
    source           TEXT NOT NULL,
    calling_ae       TEXT,
    received_at      TEXT NOT NULL
);

CREATE INDEX instances_series_fk ON instances(series_fk);
CREATE INDEX instances_blob_sha256 ON instances(blob_sha256);
CREATE INDEX series_study_fk ON series(study_fk);
