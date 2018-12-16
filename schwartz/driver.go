package schwartz

import (
	"database/sql"
)

type Repository interface {
	FindFuncMaps(db *sql.DB) ([]*FuncMap, error)
	FindFuncMapByName(db *sql.DB, funcname string) (*FuncMap, error)
	AddFuncMap(db *sql.DB, funcmap *FuncMap) error
	FindJob(db *sql.DB, jobID int) (*Job, error)
}

type FuncMap struct {
	ID   int
	Name string
}

type RepositoryOnRDB struct {
}

func (r RepositoryOnRDB) AddFuncMap(db *sql.DB, funcmap *FuncMap) error {
	_, err := db.Exec("")
	return err
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
	err := db.QueryRow(stmt, jobID).Scan(&job.JobID, &job.FuncID, &job.Arg, &job.UniqKey, &job.InsertTime,
		&job.RunAfter, &job.GrabbedUntil, &job.Priority, &job.Coalesce)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return job, nil
}
