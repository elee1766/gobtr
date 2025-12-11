package queries

import (
	"database/sql"
	"time"
)

type FilesystemError struct {
	ID        int64
	Device    string
	ErrorType string
	Message   sql.NullString
	Timestamp time.Time
	Inode     sql.NullInt64
	Path      sql.NullString
}

func InsertError(db *sql.DB, e *FilesystemError) error {
	result, err := db.Exec(`
		INSERT INTO filesystem_errors (device, error_type, message, timestamp, inode, path)
		VALUES (?, ?, ?, ?, ?, ?)
	`, e.Device, e.ErrorType, e.Message, e.Timestamp.Unix(), e.Inode, e.Path)
	if err != nil {
		return err
	}
	e.ID, err = result.LastInsertId()
	return err
}

func ListErrors(db *sql.DB, device string, since time.Time, limit int) ([]*FilesystemError, error) {
	query := `
		SELECT id, device, error_type, message, timestamp, inode, path
		FROM filesystem_errors
		WHERE 1=1
	`
	args := []interface{}{}

	if device != "" {
		query += " AND device = ?"
		args = append(args, device)
	}

	if !since.IsZero() {
		query += " AND timestamp >= ?"
		args = append(args, since.Unix())
	}

	query += " ORDER BY timestamp DESC"

	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
	}

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var errors []*FilesystemError
	for rows.Next() {
		var e FilesystemError
		var timestamp int64
		err := rows.Scan(&e.ID, &e.Device, &e.ErrorType, &e.Message, &timestamp, &e.Inode, &e.Path)
		if err != nil {
			return nil, err
		}
		e.Timestamp = time.Unix(timestamp, 0)
		errors = append(errors, &e)
	}

	return errors, rows.Err()
}

func CountErrors(db *sql.DB, device string, since time.Time) (int64, error) {
	query := "SELECT COUNT(*) FROM filesystem_errors WHERE 1=1"
	args := []interface{}{}

	if device != "" {
		query += " AND device = ?"
		args = append(args, device)
	}

	if !since.IsZero() {
		query += " AND timestamp >= ?"
		args = append(args, since.Unix())
	}

	var count int64
	err := db.QueryRow(query, args...).Scan(&count)
	return count, err
}
