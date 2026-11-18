CREATE TABLE derived_htj2k (
  instance_fk   INTEGER PRIMARY KEY REFERENCES instances(id) ON DELETE CASCADE,
  source_blob   TEXT NOT NULL,
  codec_version INTEGER NOT NULL,
  state         TEXT NOT NULL CHECK (state IN ('ready','not_eligible','failed')),
  problem       TEXT,
  blob_sha256   TEXT,
  frames        BLOB
);

CREATE INDEX derived_htj2k_blob ON derived_htj2k(blob_sha256);
