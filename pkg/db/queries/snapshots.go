package queries

import (
	"database/sql"
	"time"
)

type Snapshot struct {
	ID         string
	Path       string
	ParentUUID sql.NullString
	UUID       sql.NullString
	CreatedAt  time.Time
	IsReadonly bool
	SizeBytes  sql.NullInt64
	SourcePath sql.NullString
}

func InsertSnapshot(db *sql.DB, s *Snapshot) error {
	_, err := db.Exec(`
		INSERT INTO snapshots (id, path, parent_uuid, uuid, created_at, is_readonly, size_bytes, source_path)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, s.ID, s.Path, s.ParentUUID, s.UUID, s.CreatedAt.Unix(), s.IsReadonly, s.SizeBytes, s.SourcePath)
	return err
}

func ListSnapshots(db *sql.DB, sourcePath string) ([]*Snapshot, error) {
	query := `
		SELECT id, path, parent_uuid, uuid, created_at, is_readonly, size_bytes, source_path
		FROM snapshots
		WHERE 1=1
	`
	args := []interface{}{}

	if sourcePath != "" {
		query += " AND (source_path = ? OR path LIKE ?)"
		args = append(args, sourcePath, sourcePath+"/%")
	}

	query += " ORDER BY created_at DESC"

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var snapshots []*Snapshot
	for rows.Next() {
		var s Snapshot
		var createdAt int64
		err := rows.Scan(&s.ID, &s.Path, &s.ParentUUID, &s.UUID, &createdAt, &s.IsReadonly, &s.SizeBytes, &s.SourcePath)
		if err != nil {
			return nil, err
		}
		s.CreatedAt = time.Unix(createdAt, 0)
		snapshots = append(snapshots, &s)
	}

	return snapshots, rows.Err()
}

func DeleteSnapshot(db *sql.DB, path string) error {
	_, err := db.Exec("DELETE FROM snapshots WHERE path = ?", path)
	return err
}

func GetSnapshot(db *sql.DB, path string) (*Snapshot, error) {
	var s Snapshot
	var createdAt int64
	err := db.QueryRow(`
		SELECT id, path, parent_uuid, uuid, created_at, is_readonly, size_bytes, source_path
		FROM snapshots
		WHERE path = ?
	`, path).Scan(&s.ID, &s.Path, &s.ParentUUID, &s.UUID, &createdAt, &s.IsReadonly, &s.SizeBytes, &s.SourcePath)
	if err != nil {
		return nil, err
	}
	s.CreatedAt = time.Unix(createdAt, 0)
	return &s, nil
}
