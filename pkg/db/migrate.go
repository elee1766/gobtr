package db

import (
	"embed"

	"github.com/pressly/goose/v3"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// RunMigrations runs all pending migrations using goose
func (db *DB) RunMigrations() error {
	goose.SetBaseFS(migrationsFS)

	if err := goose.SetDialect("sqlite3"); err != nil {
		return err
	}

	// Log current version before migrating
	version, err := goose.GetDBVersion(db.conn)
	if err != nil {
		db.logger.Info("no existing migration version", "error", err)
	} else {
		db.logger.Info("current migration version", "version", version)
	}

	return goose.Up(db.conn, "migrations")
}

// ResetDatabase drops all tables and reruns migrations
func (db *DB) ResetDatabase() error {
	db.logger.Warn("resetting database - all data will be lost!")

	goose.SetBaseFS(migrationsFS)

	if err := goose.SetDialect("sqlite3"); err != nil {
		return err
	}

	// Down to version 0
	if err := goose.DownTo(db.conn, "migrations", 0); err != nil {
		return err
	}

	// Back up
	return goose.Up(db.conn, "migrations")
}

// GetMigrationVersion returns the current migration version
func (db *DB) GetMigrationVersion() (int64, error) {
	goose.SetBaseFS(migrationsFS)

	if err := goose.SetDialect("sqlite3"); err != nil {
		return 0, err
	}

	return goose.GetDBVersion(db.conn)
}
