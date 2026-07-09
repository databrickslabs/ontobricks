-- Reference DDL for one OntoBricks flat triple table on Lakebase Postgres.
-- Replace <schema> and <table> with validated identifiers (see LakebaseFlatStore).
-- Requires the pgcrypto extension for digest().

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SCHEMA IF NOT EXISTS "<schema>";

CREATE TABLE IF NOT EXISTS "<schema>"."<table>" (
    subject     TEXT NOT NULL,
    predicate   TEXT NOT NULL,
    object      TEXT NOT NULL,
    object_hash BYTEA GENERATED ALWAYS AS (digest(coalesce(object, ''), 'sha256')) STORED,
    datatype    TEXT,
    lang        TEXT,
    PRIMARY KEY (subject, predicate, object_hash)
);

CREATE INDEX IF NOT EXISTS ix_<table>_sp  ON "<schema>"."<table>" (subject, predicate);
CREATE INDEX IF NOT EXISTS ix_<table>_po  ON "<schema>"."<table>" (predicate, object_hash);
CREATE INDEX IF NOT EXISTS ix_<table>_oph ON "<schema>"."<table>" (object_hash, predicate);
