-- Description: Schema for the barecat database


--####################################  Tables
CREATE TABLE dirs
(
    path           TEXT NOT NULL,
    parent         TEXT GENERATED ALWAYS AS (
        CASE
            WHEN path = '' THEN NULL
            ELSE rtrim(rtrim(path, replace(path, '/', '')), '/')
            END
        ) VIRTUAL REFERENCES dirs (path) ON DELETE RESTRICT,

    num_subdirs    INTEGER DEFAULT 0, -- These are maintained by triggers
    num_files      INTEGER DEFAULT 0,
    num_files_tree INTEGER DEFAULT 0,
    size_tree      INTEGER DEFAULT 0,

    mode           INTEGER DEFAULT NULL,
    uid            INTEGER DEFAULT NULL,
    gid            INTEGER DEFAULT NULL,
    mtime_ns       INTEGER DEFAULT NULL
);

CREATE TABLE files
(
    path     TEXT NOT NULL,
    parent   TEXT GENERATED ALWAYS AS ( -- Parent directory is computed automatically
        rtrim(rtrim(path, replace(path, '/', '')), '/')
        ) VIRTUAL             NOT NULL REFERENCES dirs (path) ON DELETE RESTRICT,

    shard    INTEGER          NOT NULL,
    offset   INTEGER          NOT NULL,
    size     INTEGER DEFAULT 0,
    crc32c   INTEGER DEFAULT NULL,

    mode     INTEGER DEFAULT NULL,
    uid      INTEGER DEFAULT NULL,
    gid      INTEGER DEFAULT NULL,
    mtime_ns INTEGER DEFAULT NULL
);

CREATE TABLE config
(
    key        TEXT PRIMARY KEY,
    value_text TEXT    DEFAULT NULL,
    value_int  INTEGER DEFAULT NULL
) WITHOUT ROWID;

INSERT INTO config (key, value_int)
VALUES ('use_triggers', 1),
       ('shard_size_limit', CAST(power(2, 63) - 1 AS INTEGER)),
       ('schema_version_major', 0),
       ('schema_version_minor', 3);
