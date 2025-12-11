package db

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/elee1766/gobtr/pkg/config"
	_ "github.com/ncruces/go-sqlite3/driver"
	_ "github.com/ncruces/go-sqlite3/embed"
	"go.uber.org/fx"
)

var Module = fx.Module("db",
	fx.Provide(New),
)

type DB struct {
	conn   *sql.DB
	logger *slog.Logger
}

func New(lc fx.Lifecycle, cfg *config.Config, logger *slog.Logger) (*DB, error) {
	logger = logger.With("component", "db")

	// Ensure db directory exists
	dbDir := filepath.Dir(cfg.DBPath)
	if err := os.MkdirAll(dbDir, 0755); err != nil {
		return nil, err
	}

	conn, err := sql.Open("sqlite3", cfg.DBPath)
	if err != nil {
		return nil, err
	}

	db := &DB{
		conn:   conn,
		logger: logger,
	}

	if err := db.init(); err != nil {
		conn.Close()
		return nil, err
	}

	logger.Info("database initialized", "path", cfg.DBPath)

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			logger.Info("closing database")
			return db.Close()
		},
	})

	return db, nil
}

func (db *DB) init() error {
	db.logger.Debug("initializing database with migrations")

	// Enable foreign keys
	if _, err := db.conn.Exec("PRAGMA foreign_keys = ON"); err != nil {
		return err
	}

	// Run migrations
	return db.RunMigrations()
}

func (db *DB) Close() error {
	return db.conn.Close()
}

func (db *DB) Conn() *sql.DB {
	return db.conn
}

// TrackedFilesystem represents a filesystem being monitored
type TrackedFilesystem struct {
	ID               int64
	UUID             string
	Path             string
	Label            string
	BtrbkSnapshotDir string
	CreatedAt        int64
	UpdatedAt        int64
}

// AddFilesystem adds a new filesystem to track
func (db *DB) AddFilesystem(uuid, path, label, btrbkSnapshotDir string) (*TrackedFilesystem, error) {
	result, err := db.conn.Exec(
		"INSERT INTO tracked_filesystems (uuid, path, label, btrbk_snapshot_dir) VALUES (?, ?, ?, ?)",
		uuid, path, label, btrbkSnapshotDir,
	)
	if err != nil {
		return nil, err
	}

	id, err := result.LastInsertId()
	if err != nil {
		return nil, err
	}

	return db.GetFilesystem(id)
}

// GetFilesystemByUUID gets a filesystem by its btrfs UUID
func (db *DB) GetFilesystemByUUID(uuid string) (*TrackedFilesystem, error) {
	row := db.conn.QueryRow(
		"SELECT id, uuid, path, label, btrbk_snapshot_dir, created_at, updated_at FROM tracked_filesystems WHERE uuid = ?",
		uuid,
	)

	fs := &TrackedFilesystem{}
	var label, btrbkDir, fsUUID sql.NullString
	err := row.Scan(&fs.ID, &fsUUID, &fs.Path, &label, &btrbkDir, &fs.CreatedAt, &fs.UpdatedAt)
	if err != nil {
		return nil, err
	}
	if fsUUID.Valid {
		fs.UUID = fsUUID.String
	}
	if label.Valid {
		fs.Label = label.String
	}
	if btrbkDir.Valid {
		fs.BtrbkSnapshotDir = btrbkDir.String
	}
	return fs, nil
}

// GetFilesystem gets a filesystem by ID
func (db *DB) GetFilesystem(id int64) (*TrackedFilesystem, error) {
	row := db.conn.QueryRow(
		"SELECT id, uuid, path, label, btrbk_snapshot_dir, created_at, updated_at FROM tracked_filesystems WHERE id = ?",
		id,
	)

	fs := &TrackedFilesystem{}
	var label, btrbkDir, fsUUID sql.NullString
	err := row.Scan(&fs.ID, &fsUUID, &fs.Path, &label, &btrbkDir, &fs.CreatedAt, &fs.UpdatedAt)
	if err != nil {
		return nil, err
	}
	if fsUUID.Valid {
		fs.UUID = fsUUID.String
	}
	if label.Valid {
		fs.Label = label.String
	}
	if btrbkDir.Valid {
		fs.BtrbkSnapshotDir = btrbkDir.String
	}
	return fs, nil
}

// ListFilesystems returns all tracked filesystems
func (db *DB) ListFilesystems() ([]*TrackedFilesystem, error) {
	rows, err := db.conn.Query(
		"SELECT id, uuid, path, label, btrbk_snapshot_dir, created_at, updated_at FROM tracked_filesystems ORDER BY created_at",
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var filesystems []*TrackedFilesystem
	for rows.Next() {
		fs := &TrackedFilesystem{}
		var label, btrbkDir, fsUUID sql.NullString
		if err := rows.Scan(&fs.ID, &fsUUID, &fs.Path, &label, &btrbkDir, &fs.CreatedAt, &fs.UpdatedAt); err != nil {
			return nil, err
		}
		if fsUUID.Valid {
			fs.UUID = fsUUID.String
		}
		if label.Valid {
			fs.Label = label.String
		}
		if btrbkDir.Valid {
			fs.BtrbkSnapshotDir = btrbkDir.String
		}
		filesystems = append(filesystems, fs)
	}
	return filesystems, rows.Err()
}

// RemoveFilesystem removes a tracked filesystem by ID
func (db *DB) RemoveFilesystem(id int64) error {
	_, err := db.conn.Exec("DELETE FROM tracked_filesystems WHERE id = ?", id)
	return err
}

// UpdateFilesystem updates a tracked filesystem's label and btrbk snapshot dir
func (db *DB) UpdateFilesystem(id int64, label, btrbkSnapshotDir string) error {
	_, err := db.conn.Exec(
		"UPDATE tracked_filesystems SET label = ?, btrbk_snapshot_dir = ?, updated_at = strftime('%s', 'now') WHERE id = ?",
		label, btrbkSnapshotDir, id,
	)
	return err
}

// UpdateFilesystemPath updates a tracked filesystem's path (mountpoint)
func (db *DB) UpdateFilesystemPath(id int64, path string) error {
	_, err := db.conn.Exec(
		"UPDATE tracked_filesystems SET path = ?, updated_at = strftime('%s', 'now') WHERE id = ?",
		path, id,
	)
	return err
}
