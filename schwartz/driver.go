package schwartz

import (
	"database/sql"
)

type FuncMapRepository interface {
	FindFuncMaps(db *sql.DB) ([]*FuncMap, error)
}

type FuncMap struct {
	ID   int
	Name string
}

type FuncMapRepositoryOnRDB struct {
}

func (r FuncMapRepositoryOnRDB) FindAll(db *sql.DB) ([]*FuncMap, error) {
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
