#pragma once
#include <sqlite3.h>
#include <stdio.h>

struct BarecatContext {
    sqlite3 *db;
    sqlite3_stmt *stmt_get_file;
    FILE **shard_files;
    size_t num_shards;
};

int barecat_init(struct BarecatContext *ctx, const char *db_path, const char **shard_paths, size_t num_shards);
int barecat_destroy(struct BarecatContext *ctx);

int barecat_read(struct BarecatContext *ctx, const char *path, void **buf, size_t *size);
