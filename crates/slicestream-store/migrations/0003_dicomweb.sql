ALTER TABLE studies ADD COLUMN study_time TEXT;
ALTER TABLE studies ADD COLUMN study_id TEXT;
ALTER TABLE studies ADD COLUMN referring_physician_name TEXT;
ALTER TABLE studies ADD COLUMN referring_physician_name_folded TEXT;
ALTER TABLE studies ADD COLUMN patient_name TEXT;
ALTER TABLE studies ADD COLUMN patient_name_folded TEXT;
ALTER TABLE studies ADD COLUMN patient_birth_date TEXT;
ALTER TABLE studies ADD COLUMN patient_sex TEXT;

ALTER TABLE series ADD COLUMN performed_procedure_step_start_date TEXT;
ALTER TABLE series ADD COLUMN performed_procedure_step_start_time TEXT;

ALTER TABLE instances ADD COLUMN scan_version INTEGER NOT NULL DEFAULT 0;
ALTER TABLE instances ADD COLUMN scan_problem TEXT;

CREATE TABLE instance_data (
    instance_fk   INTEGER PRIMARY KEY REFERENCES instances(id),
    metadata      BLOB,
    frames        BLOB,
    bulk          TEXT NOT NULL,
    offsets_valid INTEGER NOT NULL
);

CREATE INDEX instances_scan_version ON instances(scan_version);
CREATE INDEX studies_study_date ON studies(study_date);
CREATE INDEX studies_patient_name_folded ON studies(patient_name_folded);
CREATE INDEX studies_accession_number ON studies(accession_number);
