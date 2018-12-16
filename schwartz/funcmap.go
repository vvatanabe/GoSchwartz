package schwartz

import "database/sql"

func (s *Schwartz) CreateOrFind(db *sql.DB, funcname string) (*FuncMap, error) {
	fm, err := s.repository.FindFuncMapByName(db, funcname)
	if err != nil {
		return nil, err
	}
	if fm != nil {
		return fm, nil
	}

}
