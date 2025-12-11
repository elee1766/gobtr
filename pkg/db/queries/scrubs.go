package queries

import (
	"database/sql"
	"time"
)

type ScrubHistory struct {
	ScrubID              string
	DevicePath           string
	StartedAt            time.Time
	FinishedAt           sql.NullTime
	Status               string
	BytesScrubbed        sql.NullInt64
	TotalBytes           sql.NullInt64
	DataErrors           int32
	TreeErrors           int32
	CorrectedErrors      int32
	UncorrectableErrors  int32
	// Flags used when starting the scrub
	FlagReadonly         bool
	FlagLimitBytesPerSec int64
	FlagForce            bool
}

func InsertScrub(db *sql.DB, s *ScrubHistory) error {
	var finishedAt interface{}
	if s.FinishedAt.Valid {
		finishedAt = s.FinishedAt.Time.Unix()
	}

	_, err := db.Exec(`
		INSERT INTO scrub_history (
			scrub_id, device_path, started_at, finished_at, status,
			bytes_scrubbed, total_bytes, data_errors, tree_errors,
			corrected_errors, uncorrectable_errors,
			flag_readonly, flag_limit_bytes_per_sec, flag_force
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, s.ScrubID, s.DevicePath, s.StartedAt.Unix(), finishedAt, s.Status,
		s.BytesScrubbed, s.TotalBytes, s.DataErrors, s.TreeErrors,
		s.CorrectedErrors, s.UncorrectableErrors,
		s.FlagReadonly, s.FlagLimitBytesPerSec, s.FlagForce)
	return err
}

func UpdateScrub(db *sql.DB, s *ScrubHistory) error {
	var finishedAt interface{}
	if s.FinishedAt.Valid {
		finishedAt = s.FinishedAt.Time.Unix()
	}

	_, err := db.Exec(`
		UPDATE scrub_history
		SET finished_at = ?, status = ?, bytes_scrubbed = ?, total_bytes = ?,
		    data_errors = ?, tree_errors = ?, corrected_errors = ?, uncorrectable_errors = ?
		WHERE scrub_id = ?
	`, finishedAt, s.Status, s.BytesScrubbed, s.TotalBytes,
		s.DataErrors, s.TreeErrors, s.CorrectedErrors, s.UncorrectableErrors, s.ScrubID)
	return err
}

func GetScrub(db *sql.DB, scrubID string) (*ScrubHistory, error) {
	var s ScrubHistory
	var startedAt, finishedAt sql.NullInt64

	err := db.QueryRow(`
		SELECT scrub_id, device_path, started_at, finished_at, status,
		       bytes_scrubbed, total_bytes, data_errors, tree_errors,
		       corrected_errors, uncorrectable_errors,
		       COALESCE(flag_readonly, 0), COALESCE(flag_limit_bytes_per_sec, 0), COALESCE(flag_force, 0)
		FROM scrub_history
		WHERE scrub_id = ?
	`, scrubID).Scan(
		&s.ScrubID, &s.DevicePath, &startedAt, &finishedAt, &s.Status,
		&s.BytesScrubbed, &s.TotalBytes, &s.DataErrors, &s.TreeErrors,
		&s.CorrectedErrors, &s.UncorrectableErrors,
		&s.FlagReadonly, &s.FlagLimitBytesPerSec, &s.FlagForce,
	)
	if err != nil {
		return nil, err
	}

	if startedAt.Valid {
		s.StartedAt = time.Unix(startedAt.Int64, 0)
	}
	if finishedAt.Valid {
		s.FinishedAt = sql.NullTime{Time: time.Unix(finishedAt.Int64, 0), Valid: true}
	}

	return &s, nil
}

func ListScrubHistory(db *sql.DB, devicePath string, limit int) ([]*ScrubHistory, error) {
	query := `
		SELECT scrub_id, device_path, started_at, finished_at, status,
		       bytes_scrubbed, total_bytes, data_errors, tree_errors,
		       corrected_errors, uncorrectable_errors,
		       COALESCE(flag_readonly, 0), COALESCE(flag_limit_bytes_per_sec, 0), COALESCE(flag_force, 0)
		FROM scrub_history
		WHERE 1=1
	`
	args := []interface{}{}

	if devicePath != "" {
		query += " AND device_path = ?"
		args = append(args, devicePath)
	}

	query += " ORDER BY started_at DESC"

	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
	}

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var scrubs []*ScrubHistory
	for rows.Next() {
		var s ScrubHistory
		var startedAt, finishedAt sql.NullInt64

		err := rows.Scan(
			&s.ScrubID, &s.DevicePath, &startedAt, &finishedAt, &s.Status,
			&s.BytesScrubbed, &s.TotalBytes, &s.DataErrors, &s.TreeErrors,
			&s.CorrectedErrors, &s.UncorrectableErrors,
			&s.FlagReadonly, &s.FlagLimitBytesPerSec, &s.FlagForce,
		)
		if err != nil {
			return nil, err
		}

		if startedAt.Valid {
			s.StartedAt = time.Unix(startedAt.Int64, 0)
		}
		if finishedAt.Valid {
			s.FinishedAt = sql.NullTime{Time: time.Unix(finishedAt.Int64, 0), Valid: true}
		}

		scrubs = append(scrubs, &s)
	}

	return scrubs, rows.Err()
}

func GetActiveScrub(db *sql.DB, devicePath string) (*ScrubHistory, error) {
	var s ScrubHistory
	var startedAt, finishedAt sql.NullInt64

	err := db.QueryRow(`
		SELECT scrub_id, device_path, started_at, finished_at, status,
		       bytes_scrubbed, total_bytes, data_errors, tree_errors,
		       corrected_errors, uncorrectable_errors,
		       COALESCE(flag_readonly, 0), COALESCE(flag_limit_bytes_per_sec, 0), COALESCE(flag_force, 0)
		FROM scrub_history
		WHERE device_path = ? AND status IN ('running', 'starting')
		ORDER BY started_at DESC
		LIMIT 1
	`, devicePath).Scan(
		&s.ScrubID, &s.DevicePath, &startedAt, &finishedAt, &s.Status,
		&s.BytesScrubbed, &s.TotalBytes, &s.DataErrors, &s.TreeErrors,
		&s.CorrectedErrors, &s.UncorrectableErrors,
		&s.FlagReadonly, &s.FlagLimitBytesPerSec, &s.FlagForce,
	)
	if err != nil {
		return nil, err
	}

	if startedAt.Valid {
		s.StartedAt = time.Unix(startedAt.Int64, 0)
	}
	if finishedAt.Valid {
		s.FinishedAt = sql.NullTime{Time: time.Unix(finishedAt.Int64, 0), Valid: true}
	}

	return &s, nil
}

// UpsertScrub inserts or updates a scrub record based on scrub_id
func UpsertScrub(db *sql.DB, s *ScrubHistory) error {
	var finishedAt interface{}
	if s.FinishedAt.Valid {
		finishedAt = s.FinishedAt.Time.Unix()
	}

	_, err := db.Exec(`
		INSERT INTO scrub_history (
			scrub_id, device_path, started_at, finished_at, status,
			bytes_scrubbed, total_bytes, data_errors, tree_errors,
			corrected_errors, uncorrectable_errors,
			flag_readonly, flag_limit_bytes_per_sec, flag_force
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(scrub_id) DO UPDATE SET
			finished_at = excluded.finished_at,
			status = excluded.status,
			bytes_scrubbed = excluded.bytes_scrubbed,
			total_bytes = excluded.total_bytes,
			data_errors = excluded.data_errors,
			tree_errors = excluded.tree_errors,
			corrected_errors = excluded.corrected_errors,
			uncorrectable_errors = excluded.uncorrectable_errors
	`, s.ScrubID, s.DevicePath, s.StartedAt.Unix(), finishedAt, s.Status,
		s.BytesScrubbed, s.TotalBytes, s.DataErrors, s.TreeErrors,
		s.CorrectedErrors, s.UncorrectableErrors,
		s.FlagReadonly, s.FlagLimitBytesPerSec, s.FlagForce)
	return err
}
