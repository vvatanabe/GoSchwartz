package schwartz

import (
	"database/sql"
	"time"
)

type DB struct {
	*sql.DB
	dead         bool
	retrySeconds time.Duration
	retryAt      time.Time
}

func (db *DB) markDead() bool {
	db.dead = true
	db.retryAt = time.Now().Add(db.retrySeconds)
}

func (db *DB) isDead() bool {
	if db.dead {
		if db.retryAt.Before(time.Now()) {
			db.dead = false
			db.retryAt = time.Time{}
			return false
		}
		return true
	}
	return false
}

func (db *DB) serverTime() (*time.Time, error) {
	sql.Open()
	stmt := `SELECT UNIX_TIMESTAMP() AS TIMESTAMP`
	var now time.Time
	err := db.QueryRow(stmt).Scan(&now)
	if err != nil {
		return nil, err
	}
	return &now, nil
}

type Transaction interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Prepare(query string) (*sql.Stmt, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

type TxFn func(Transaction) error

func WithTransaction(db *sql.DB, fn TxFn) (err error) {
	tx, err := db.Begin()
	if err != nil {
		return
	}

	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p)
		} else if err != nil {
			tx.Rollback()
		} else {
			err = tx.Commit()
		}
	}()

	err = fn(tx)
	return err
}

func xxx(db *sql.DB) {
	WithTransaction(db, func(transaction Transaction) error {

	})
}

// test1
// test2
// test3