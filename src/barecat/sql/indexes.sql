-- Indexes (unique indexes replace PRIMARY KEY for droppability during bulk import)
CREATE UNIQUE INDEX idx_files_path ON files (path);
CREATE UNIQUE INDEX idx_dirs_path ON dirs (path);
CREATE INDEX idx_files_parent ON files (parent);
CREATE INDEX idx_dirs_parent ON dirs (parent);
CREATE INDEX idx_files_shard_offset ON files (shard, offset);
