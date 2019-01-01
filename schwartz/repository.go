package schwartz

import (
	"database/sql"
	"errors"
	"time"
)

type funcMapRepository interface {
	FindFuncMaps(db *DB) ([]*FuncMap, error)
	FindFuncMapByName(db *DB, funcName string) (*FuncMap, error)
	AddFuncMap(db *DB, funcMap *FuncMap) error
}

type jobRepository interface {
	FindJob(db *DB, jobID int) (*Job, error)
	FindJobsByFuncIDsOrderByJobID(db *DB, funcIDs []int, opts ...FindJobsOption) ([]*Job, error)
	AddJob(db *DB, job *Job) error
	UpdateJob(db *DB, job *Job, from time.Time) error
	Remove(db *DB, job *Job) error
}

type failureLogRepository interface {
	FindFailureLogs(db *DB, jobID int) ([]*FailureLog, error)
}

type FailureLog struct {
	ErrorTime time.Time
	JobID     int
	Message   string
	FuncID    int
}

func (r RepositoryOnRDB) FindFailureLogs(db *sql.DB, jobID int) ([]*FuncMap, error) {
	stmt := `
SELECT
	error_time, jobid, message, funcid
FROM
	error
WHERE
	jobid=?
`
	rows, err := db.Query(stmt, jobID)
	if err != nil {
		// TODO error handling
		return nil, err
	}
	defer rows.Close()
	var failureLogs []*FailureLog
	for rows.Next() {
		var f FailureLog
		if err := rows.Scan(&f.ErrorTime, &f.JobID, &f.Message, &f.FuncID); err != nil {
			// TODO error handling
			return nil, err
		}
		failureLogs = append(failureLogs, &f)
	}
	return failureLogs, nil
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

type findJobsOption struct {
	floor      int
	prioritize bool
}

type FindJobsOption func(opt *findJobsOption)

func Floor(i int) func(opt *findJobsOption) {
	return func(opt *findJobsOption) {
		opt.floor = i
	}
}

func Prioritize(b bool) func(opt *findJobsOption) {
	return func(opt *findJobsOption) {
		opt.prioritize = b
	}
}

func (r RepositoryOnRDB) FindJobsByFuncIDsOrderByJobID(db *sql.DB, funcIDs []int, opts ...FindJobsOption) ([]*Job, error) {

	var opt findJobsOption
	for _, f := range opts {
		f(&opt)
	}

	stmt := `
SELECT
	jobid, funcid, arg, uniqkey, insert_time, run_after, grabbed_until, priority, coalesce
FROM
	job
WHERE
	funcid IN (?)
	AND run_after <= UNIX_TIMESTAMP()
	AND grabbed_until <= UNIX_TIMESTAMP()
	AND priority>=?
`
	if opt.prioritize {
		stmt += `
ORDER BY priority desc, jobid
`
	} else {
		stmt += `
ORDER BY jobid
`
	}

	rows, err := db.Query(stmt, funcIDs, opt.floor)
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
