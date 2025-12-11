package btrfs

import (
	"database/sql"
	"time"

	"github.com/elee1766/gobtr/pkg/db/queries"
)

// CollectDeviceErrors collects error statistics from devices and stores them in the database
func (m *Manager) CollectDeviceErrors(db *sql.DB, devicePath string) error {
	stats, err := m.GetDeviceStats(devicePath)
	if err != nil {
		return err
	}

	for _, stat := range stats {
		// Record errors if any exist
		if stat.WriteErrors > 0 {
			err := queries.InsertError(db, &queries.FilesystemError{
				Device:    stat.DevicePath,
				ErrorType: "write_io_err",
				Message:   sql.NullString{String: "Write I/O error detected", Valid: true},
				Timestamp: time.Now(),
			})
			if err != nil {
				m.logger.Error("failed to record write error", "error", err)
			}
		}

		if stat.ReadErrors > 0 {
			err := queries.InsertError(db, &queries.FilesystemError{
				Device:    stat.DevicePath,
				ErrorType: "read_io_err",
				Message:   sql.NullString{String: "Read I/O error detected", Valid: true},
				Timestamp: time.Now(),
			})
			if err != nil {
				m.logger.Error("failed to record read error", "error", err)
			}
		}

		if stat.FlushErrors > 0 {
			err := queries.InsertError(db, &queries.FilesystemError{
				Device:    stat.DevicePath,
				ErrorType: "flush_io_err",
				Message:   sql.NullString{String: "Flush I/O error detected", Valid: true},
				Timestamp: time.Now(),
			})
			if err != nil {
				m.logger.Error("failed to record flush error", "error", err)
			}
		}

		if stat.CorruptionErrors > 0 {
			err := queries.InsertError(db, &queries.FilesystemError{
				Device:    stat.DevicePath,
				ErrorType: "corruption_err",
				Message:   sql.NullString{String: "Corruption error detected", Valid: true},
				Timestamp: time.Now(),
			})
			if err != nil {
				m.logger.Error("failed to record corruption error", "error", err)
			}
		}

		if stat.GenerationErrors > 0 {
			err := queries.InsertError(db, &queries.FilesystemError{
				Device:    stat.DevicePath,
				ErrorType: "generation_err",
				Message:   sql.NullString{String: "Generation error detected", Valid: true},
				Timestamp: time.Now(),
			})
			if err != nil {
				m.logger.Error("failed to record generation error", "error", err)
			}
		}
	}

	return nil
}
