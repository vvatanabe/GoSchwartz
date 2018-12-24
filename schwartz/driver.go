package schwartz

import (
	"database/sql"
	"errors"
	"time"
)

type Repository interface {
	FindFuncMaps(db *sql.DB) ([]*FuncMap, error)
	FindFuncMapByName(db *sql.DB, funcname string) (*FuncMap, error)
	AddFuncMap(db *sql.DB, funcmap *FuncMap) error
	FindJob(db *sql.DB, jobID int) (*Job, error)
	AddJob(db *sql.DB, job *Job) error
	UpdateJob(db *sql.DB, job *Job, from time.Time) error
	FindJobsByFuncIDsOrderByJobID(db *sql.DB, funcIDs []int) ([]*Job, error)
}

type FuncMap struct {
	ID   int64
	Name string
}

type RepositoryOnRDB struct {
}

func (r RepositoryOnRDB) FindFuncMaps(db *sql.DB) ([]*FuncMap, error) {
	stmt := `
SELECT
	funcid, funcname
FROM
	funcmap
`
	rows, err := db.Query(stmt)
	if err != nil {
		// TODO error handling
		return nil, err
	}
	defer rows.Close()
	var maps []*FuncMap
	for rows.Next() {
		var m FuncMap
		if err := rows.Scan(&m.ID, &m.Name); err != nil {
			// TODO error handling
			return nil, err
		}
		maps = append(maps, &m)
	}
	return maps, nil
}

func (r RepositoryOnRDB) FindFuncMapByName(db *sql.DB, funcname string) (*FuncMap, error) {
	stmt := `
SELECT
	funcid, funcname
FROM
	funcmap
WHERE funcname = ?
`
	var fm FuncMap
	err := db.QueryRow(stmt, funcname).Scan(&fm.ID, &fm.Name)
	if err != nil {
		// TODO error handling
		return nil, err
	}
	return &fm, nil
}

func (r RepositoryOnRDB) AddFuncMap(db *sql.DB, funcmap *FuncMap) error {
	stmt := `INSERT INTO funcmap (funcname) VALUES (?)`
	ret, err := db.Exec(stmt, funcmap.Name)
	if err != nil {
		return err
	}
	id, err := ret.LastInsertId()
	if err != nil {
		return err
	}
	funcmap.ID = id
	return nil
}

func (r RepositoryOnRDB) FindJob(db *sql.DB, jobID int) (*Job, error) {
	stmt := `
SELECT
	jobid, funcid, arg, uniqkey, insert_time, run_after, grabbed_until, priority, coalesce
FROM
  job
WHERE
  jobid = ?
`
	var job Job
	err := db.QueryRow(stmt, jobID).Scan(&job.ID, &job.FuncID, &job.Arg, &job.UniqKey, &job.InsertTime,
		&job.RunAfter, &job.GrabbedUntil, &job.Priority, &job.Coalesce)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return job, nil
}

// SELECT *
// FROM job
// WHERE funcid=? AND run_after<=UNIX_TIMESTAMP() AND grabbed_until<=UNIX_TIMESTAMP() AND priority>=?
// ORDER BY priority desc, jobid

func (r RepositoryOnRDB) FindJobsByFuncIDsOrderByJobID(db *sql.DB, funcIDs []int) ([]*Job, error) {
	stmt := `
SELECT
	jobid, funcid, arg, uniqkey, insert_time, run_after, grabbed_until, priority, coalesce
FROM
	job
WHERE
	funcid IN (?) AND run_after <= UNIX_TIMESTAMP() AND grabbed_until <= UNIX_TIMESTAMP()
ORDER BY jobid
`
	rows, err := db.Query(stmt, funcIDs)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	var jobs []*Job
	for rows.Next() {
		var job Job
		if err := rows.Scan(&job.ID, &job.FuncID, &job.Arg, &job.UniqKey, &job.InsertTime, &job.RunAfter, &job.GrabbedUntil, &job.Priority, &job.Coalesce); err != nil {
			// TODO error handling
			return nil, err
		}
		jobs = append(jobs, &job)
	}
	return jobs, nil
}

func (r RepositoryOnRDB) AddJob(db *sql.DB, job *Job) error {
	stmt := `
INSERT INTO job (funcid, arg, uniqkey, insert_time, run_after, grabbed_until, priority, coalesce)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
`
	ret, err := db.Exec(stmt, job.FuncID, job.Arg, job.UniqKey, job.InsertTime, job.RunAfter, job.GrabbedUntil, job.Priority, job.Coalesce)
	if err != nil {
		return err
	}
	id, err := ret.LastInsertId()
	if err != nil {
		return err
	}
	job.ID = id
	return nil
}

func (r RepositoryOnRDB) UpdateJob(db *sql.DB, job *Job, from time.Time) error {
	stmt := `
UPDATE job SET jobid=?, funcid=?, arg=?, uniqkey=?, insert_time=?, run_after=?, grabbed_until=?, priority=?, coalesce=?
WHERE jobid=? AND grabbed_until BETWEEN ? AND UNIX_TIMESTAMP()
`
	ret, err := db.Exec(stmt, job.ID, job.FuncID, job.Arg, job.UniqKey, job.InsertTime, job.RunAfter.Unix(), job.GrabbedUntil.Unix(), job.Priority, job.Coalesce, job.ID, from.Unix())
	if err != nil {
		return err
	}
	cnt, err := ret.RowsAffected()
	if err != nil {
		return err
	}
	if cnt < 1 {
		// TODO handle
		return errors.New("not update")
	}
	return nil
}

func (r *RepositoryOnRDB) GetServerTime(db *sql.DB) (*time.Time, error) {
	stmt := `SELECT UNIX_TIMESTAMP() AS TIMESTAMP`
	var now time.Time
	err := db.QueryRow(stmt).Scan(&now)
	if err != nil {
		return nil, err
	}
	return &now, nil
}
