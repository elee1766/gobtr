package queries

import (
	"database/sql"
	"time"
)

type BalanceHistory struct {
	BalanceID         string
	DevicePath        string
	StartedAt         time.Time
	FinishedAt        sql.NullTime
	Status            string
	ChunksConsidered  int64
	ChunksRelocated   int64
	SizeRelocated     int64
	SoftErrors        int32
	// Flags used when starting the balance
	FlagData         bool
	FlagMetadata     bool
	FlagSystem       bool
	FlagUsagePercent int32
	FlagLimitChunks  int64
	FlagLimitPercent int32
	FlagBackground   bool
	FlagDryRun       bool
	FlagForce        bool
}

func InsertBalance(db *sql.DB, b *BalanceHistory) error {
	var finishedAt interface{}
	if b.FinishedAt.Valid {
		finishedAt = b.FinishedAt.Time.Unix()
	}

	_, err := db.Exec(`
		INSERT INTO balance_history (
			balance_id, device_path, started_at, finished_at, status,
			chunks_considered, chunks_relocated, size_relocated, soft_errors,
			flag_data, flag_metadata, flag_system, flag_usage_percent,
			flag_limit_chunks, flag_limit_percent, flag_background, flag_dry_run, flag_force
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, b.BalanceID, b.DevicePath, b.StartedAt.Unix(), finishedAt, b.Status,
		b.ChunksConsidered, b.ChunksRelocated, b.SizeRelocated, b.SoftErrors,
		b.FlagData, b.FlagMetadata, b.FlagSystem, b.FlagUsagePercent,
		b.FlagLimitChunks, b.FlagLimitPercent, b.FlagBackground, b.FlagDryRun, b.FlagForce)
	return err
}

func UpdateBalance(db *sql.DB, b *BalanceHistory) error {
	var finishedAt interface{}
	if b.FinishedAt.Valid {
		finishedAt = b.FinishedAt.Time.Unix()
	}

	_, err := db.Exec(`
		UPDATE balance_history
		SET finished_at = ?, status = ?, chunks_considered = ?, chunks_relocated = ?,
		    size_relocated = ?, soft_errors = ?
		WHERE balance_id = ?
	`, finishedAt, b.Status, b.ChunksConsidered, b.ChunksRelocated,
		b.SizeRelocated, b.SoftErrors, b.BalanceID)
	return err
}

func GetBalance(db *sql.DB, balanceID string) (*BalanceHistory, error) {
	var b BalanceHistory
	var startedAt, finishedAt sql.NullInt64

	err := db.QueryRow(`
		SELECT balance_id, device_path, started_at, finished_at, status,
		       chunks_considered, chunks_relocated, size_relocated, soft_errors,
		       COALESCE(flag_data, 0), COALESCE(flag_metadata, 0), COALESCE(flag_system, 0),
		       COALESCE(flag_usage_percent, 0), COALESCE(flag_limit_chunks, 0),
		       COALESCE(flag_limit_percent, 0), COALESCE(flag_background, 0),
		       COALESCE(flag_dry_run, 0), COALESCE(flag_force, 0)
		FROM balance_history
		WHERE balance_id = ?
	`, balanceID).Scan(
		&b.BalanceID, &b.DevicePath, &startedAt, &finishedAt, &b.Status,
		&b.ChunksConsidered, &b.ChunksRelocated, &b.SizeRelocated, &b.SoftErrors,
		&b.FlagData, &b.FlagMetadata, &b.FlagSystem, &b.FlagUsagePercent,
		&b.FlagLimitChunks, &b.FlagLimitPercent, &b.FlagBackground, &b.FlagDryRun, &b.FlagForce,
	)
	if err != nil {
		return nil, err
	}

	if startedAt.Valid {
		b.StartedAt = time.Unix(startedAt.Int64, 0)
	}
	if finishedAt.Valid {
		b.FinishedAt = sql.NullTime{Time: time.Unix(finishedAt.Int64, 0), Valid: true}
	}

	return &b, nil
}

func ListBalanceHistory(db *sql.DB, devicePath string, limit int) ([]*BalanceHistory, error) {
	query := `
		SELECT balance_id, device_path, started_at, finished_at, status,
		       chunks_considered, chunks_relocated, size_relocated, soft_errors,
		       COALESCE(flag_data, 0), COALESCE(flag_metadata, 0), COALESCE(flag_system, 0),
		       COALESCE(flag_usage_percent, 0), COALESCE(flag_limit_chunks, 0),
		       COALESCE(flag_limit_percent, 0), COALESCE(flag_background, 0),
		       COALESCE(flag_dry_run, 0), COALESCE(flag_force, 0)
		FROM balance_history
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

	var balances []*BalanceHistory
	for rows.Next() {
		var b BalanceHistory
		var startedAt, finishedAt sql.NullInt64

		err := rows.Scan(
			&b.BalanceID, &b.DevicePath, &startedAt, &finishedAt, &b.Status,
			&b.ChunksConsidered, &b.ChunksRelocated, &b.SizeRelocated, &b.SoftErrors,
			&b.FlagData, &b.FlagMetadata, &b.FlagSystem, &b.FlagUsagePercent,
			&b.FlagLimitChunks, &b.FlagLimitPercent, &b.FlagBackground, &b.FlagDryRun, &b.FlagForce,
		)
		if err != nil {
			return nil, err
		}

		if startedAt.Valid {
			b.StartedAt = time.Unix(startedAt.Int64, 0)
		}
		if finishedAt.Valid {
			b.FinishedAt = sql.NullTime{Time: time.Unix(finishedAt.Int64, 0), Valid: true}
		}

		balances = append(balances, &b)
	}

	return balances, rows.Err()
}

func GetActiveBalance(db *sql.DB, devicePath string) (*BalanceHistory, error) {
	var b BalanceHistory
	var startedAt, finishedAt sql.NullInt64

	err := db.QueryRow(`
		SELECT balance_id, device_path, started_at, finished_at, status,
		       chunks_considered, chunks_relocated, size_relocated, soft_errors,
		       COALESCE(flag_data, 0), COALESCE(flag_metadata, 0), COALESCE(flag_system, 0),
		       COALESCE(flag_usage_percent, 0), COALESCE(flag_limit_chunks, 0),
		       COALESCE(flag_limit_percent, 0), COALESCE(flag_background, 0),
		       COALESCE(flag_dry_run, 0), COALESCE(flag_force, 0)
		FROM balance_history
		WHERE device_path = ? AND status IN ('running', 'starting', 'paused')
		ORDER BY started_at DESC
		LIMIT 1
	`, devicePath).Scan(
		&b.BalanceID, &b.DevicePath, &startedAt, &finishedAt, &b.Status,
		&b.ChunksConsidered, &b.ChunksRelocated, &b.SizeRelocated, &b.SoftErrors,
		&b.FlagData, &b.FlagMetadata, &b.FlagSystem, &b.FlagUsagePercent,
		&b.FlagLimitChunks, &b.FlagLimitPercent, &b.FlagBackground, &b.FlagDryRun, &b.FlagForce,
	)
	if err != nil {
		return nil, err
	}

	if startedAt.Valid {
		b.StartedAt = time.Unix(startedAt.Int64, 0)
	}
	if finishedAt.Valid {
		b.FinishedAt = sql.NullTime{Time: time.Unix(finishedAt.Int64, 0), Valid: true}
	}

	return &b, nil
}

// UpsertBalance inserts or updates a balance record based on balance_id
func UpsertBalance(db *sql.DB, b *BalanceHistory) error {
	var finishedAt interface{}
	if b.FinishedAt.Valid {
		finishedAt = b.FinishedAt.Time.Unix()
	}

	_, err := db.Exec(`
		INSERT INTO balance_history (
			balance_id, device_path, started_at, finished_at, status,
			chunks_considered, chunks_relocated, size_relocated, soft_errors,
			flag_data, flag_metadata, flag_system, flag_usage_percent,
			flag_limit_chunks, flag_limit_percent, flag_background, flag_dry_run, flag_force
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(balance_id) DO UPDATE SET
			finished_at = excluded.finished_at,
			status = excluded.status,
			chunks_considered = excluded.chunks_considered,
			chunks_relocated = excluded.chunks_relocated,
			size_relocated = excluded.size_relocated,
			soft_errors = excluded.soft_errors
	`, b.BalanceID, b.DevicePath, b.StartedAt.Unix(), finishedAt, b.Status,
		b.ChunksConsidered, b.ChunksRelocated, b.SizeRelocated, b.SoftErrors,
		b.FlagData, b.FlagMetadata, b.FlagSystem, b.FlagUsagePercent,
		b.FlagLimitChunks, b.FlagLimitPercent, b.FlagBackground, b.FlagDryRun, b.FlagForce)
	return err
}
