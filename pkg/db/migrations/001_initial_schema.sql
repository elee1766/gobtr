-- +goose Up
-- Initial schema for btrfsguid
-- UUID is the primary identifier for filesystems

-- Tracked filesystems (btrfs mounts we monitor)
CREATE TABLE tracked_filesystems (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    uuid TEXT NOT NULL UNIQUE,
    path TEXT NOT NULL,
    label TEXT,
    btrbk_snapshot_dir TEXT,
    created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
    updated_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_tracked_filesystems_uuid ON tracked_filesystems(uuid);
CREATE INDEX IF NOT EXISTS idx_tracked_filesystems_path ON tracked_filesystems(path);

-- Snapshots
CREATE TABLE IF NOT EXISTS snapshots (
    id TEXT PRIMARY KEY,
    path TEXT NOT NULL UNIQUE,
    parent_uuid TEXT,
    uuid TEXT,
    created_at INTEGER NOT NULL,
    is_readonly INTEGER NOT NULL DEFAULT 1,
    size_bytes INTEGER,
    source_path TEXT
);

CREATE INDEX IF NOT EXISTS idx_snapshots_created_at ON snapshots(created_at);
CREATE INDEX IF NOT EXISTS idx_snapshots_source_path ON snapshots(source_path);

-- Filesystem errors
CREATE TABLE IF NOT EXISTS filesystem_errors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    device TEXT NOT NULL,
    error_type TEXT NOT NULL,
    message TEXT,
    timestamp INTEGER NOT NULL,
    inode INTEGER,
    path TEXT
);

CREATE INDEX IF NOT EXISTS idx_errors_device ON filesystem_errors(device);
CREATE INDEX IF NOT EXISTS idx_errors_timestamp ON filesystem_errors(timestamp);

-- Scrub history
CREATE TABLE IF NOT EXISTS scrub_history (
    scrub_id TEXT PRIMARY KEY,
    device_path TEXT NOT NULL,
    started_at INTEGER NOT NULL,
    finished_at INTEGER,
    status TEXT NOT NULL,
    bytes_scrubbed INTEGER,
    total_bytes INTEGER,
    data_errors INTEGER DEFAULT 0,
    tree_errors INTEGER DEFAULT 0,
    corrected_errors INTEGER DEFAULT 0,
    uncorrectable_errors INTEGER DEFAULT 0,
    flag_readonly INTEGER DEFAULT 0,
    flag_limit_bytes_per_sec INTEGER DEFAULT 0,
    flag_force INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_scrub_device ON scrub_history(device_path);
CREATE INDEX IF NOT EXISTS idx_scrub_started ON scrub_history(started_at);

-- Device stats snapshots
CREATE TABLE IF NOT EXISTS device_stats (
    device_path TEXT NOT NULL,
    timestamp INTEGER NOT NULL,
    total_bytes INTEGER,
    used_bytes INTEGER,
    free_bytes INTEGER,
    write_errors INTEGER DEFAULT 0,
    read_errors INTEGER DEFAULT 0,
    flush_errors INTEGER DEFAULT 0,
    corruption_errors INTEGER DEFAULT 0,
    generation_errors INTEGER DEFAULT 0,
    PRIMARY KEY (device_path, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_device_stats_timestamp ON device_stats(timestamp);

-- Balance history
CREATE TABLE IF NOT EXISTS balance_history (
    balance_id TEXT PRIMARY KEY,
    device_path TEXT NOT NULL,
    started_at INTEGER NOT NULL,
    finished_at INTEGER,
    status TEXT NOT NULL,
    chunks_considered INTEGER DEFAULT 0,
    chunks_relocated INTEGER DEFAULT 0,
    size_relocated INTEGER DEFAULT 0,
    soft_errors INTEGER DEFAULT 0,
    flag_data INTEGER DEFAULT 0,
    flag_metadata INTEGER DEFAULT 0,
    flag_system INTEGER DEFAULT 0,
    flag_usage_percent INTEGER DEFAULT 0,
    flag_limit_chunks INTEGER DEFAULT 0,
    flag_limit_percent INTEGER DEFAULT 0,
    flag_background INTEGER DEFAULT 0,
    flag_dry_run INTEGER DEFAULT 0,
    flag_force INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_balance_device ON balance_history(device_path);
CREATE INDEX IF NOT EXISTS idx_balance_started ON balance_history(started_at);

-- +goose Down
DROP TABLE IF EXISTS balance_history;
DROP TABLE IF EXISTS device_stats;
DROP TABLE IF EXISTS scrub_history;
DROP TABLE IF EXISTS filesystem_errors;
DROP TABLE IF EXISTS snapshots;
DROP TABLE IF EXISTS tracked_filesystems;
