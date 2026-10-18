package repository

import (
	"database/sql"

	"github.com/jmoiron/sqlx"
)

// NoSyncTx makes tx's commit not wait for the write-ahead log to reach the
// disk (synchronous_commit off, for this transaction only). The commit is
// visible right away; a crash may lose it, but never out of order: the WAL
// is sequential, so the next durable commit makes it durable too. For
// writes that are safe to lose or redo:
//
//   - rebuildable data (the Kafka projections, audit rows);
//   - a job lock (lost: the job is fetched again — at least once);
//   - a token moving through steps that don't wait (a start event, a
//     gateway): the durable commit of the wait state it reaches next
//     carries it; a crash before that leaves the token where a crash
//     already left it before.
//
// Waiting for the disk on every commit was the main cost of running an
// instance under load (measured: COMMIT dominated the database time).
func NoSyncTx(tx *sqlx.Tx) error {
	_, err := tx.Exec(`SET LOCAL synchronous_commit TO OFF`)
	return err
}

// execNoSync runs one statement in a transaction that doesn't wait for
// the disk (see NoSyncTx).
func execNoSync(db *sqlx.DB, query string, args ...interface{}) (sql.Result, error) {
	tx, err := db.Beginx()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if err := NoSyncTx(tx); err != nil {
		return nil, err
	}
	res, err := tx.Exec(query, args...)
	if err != nil {
		return nil, err
	}
	return res, tx.Commit()
}
