package schwartz

import (
	"database/sql"
	"errors"
	"log"
	"time"
)

const (
	RetryDefault     = 30
	FindJobBatchSize = 50
)

func NewSchwartz(databases Databases) *Schwartz {
	var schwartz *Schwartz
	schwartz.Databases = databases
	schwartz.RetrySeconds = RetryDefault
	schwartz.BatchSize = FindJobBatchSize
	// TODO implements
	return schwartz
}

type DatabaseName = string
type Databases = map[DatabaseName]*sql.DB

type Schwartz struct {
	Databases             Databases
	Verbose               bool
	Prioritize            int
	Floor                 int
	BatchSize             int
	DriverCacheExpiration time.Duration
	RetrySeconds          time.Duration
	StrictRemoveAbility   bool

	funcmapCache map[string]*cache

	allAbilities []interface{}
}

type cache struct {
	funcname2id map[string]int
	funcid2name map[int]string
}

type Job struct {
	JobID        int
	FuncID       int
	FuncName     string
	Arg          []byte
	UniqKey      string
	InsertTime   time.Time
	RunAfter     time.Time
	GrabbedUntil time.Time
	Priority     int
	Coalesce     string

	Handle *JobHandle
}

func (s *Schwartz) isDatabaseDead(name DatabaseName) bool {
	db, ok := s.Databases[name]
	if !ok {
		return false
	}
	if err := db.Ping(); err != nil {
		return true
	}
	return false
}

type terms struct {
	runAfter     *runAfterTerm
	grabbedUntil *grabbedUntilTerm
	jobid        *jobIDTerm
}

type runAfterTerm struct {
	OP    string
	Value time.Duration
}

type grabbedUntilTerm struct {
	OP    string
	Value time.Duration
}

type jobIDTerm struct {
	OP    string
	Value int
}

func (s *Schwartz) ListJobs(
	funcname string,
	runAfter time.Duration,
	grabbedUntil time.Duration,
	coalesceOP string,
	coalesce string,
	wantHandle bool,
	jobID int) []*Job {
	var jobs []*Job
	// TODO implements
	var terms *terms
	return jobs
}

var ErrNotFoundDB = errors.New("schwartz: Not found DB")

func (s *Schwartz) LookupJob(name DatabaseName, jobID int) (*Job, error) {
	db := s.findDB(name)
	if db == nil {
		return nil, ErrNotFoundDB
	}
	stmt := `
SELECT
	jobid, funcid, arg, uniqkey, insert_time, run_after, grabbed_until, priority, coalesce
FROM
  job
WHERE
  jobid = ?
`
	var job Job
	err := db.QueryRow(stmt, jobID).Scan(job, &job.JobID, &job.FuncID, &job.Arg, &job.UniqKey, &job.InsertTime,
		&job.RunAfter, &job.GrabbedUntil, &job.Priority, &job.Coalesce)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	job.Handle = &JobHandle{name, jobID, s}
	job.FuncName = s.funcIDToName(job.FuncID)
	return &job, nil
}

func (s *Schwartz) findDB(name DatabaseName) *sql.DB {
	db, ok := s.Databases[name]
	if !ok {
		return nil
	}
	return db
}

func (s *Schwartz) funcIDToName(name DatabaseName, funcID int) string {
	cache := s.funcMapCache(name)
	return cache.funcid2name[funcID]

}

func (s *Schwartz) funcMapCache(name DatabaseName) *cache {
	c, ok := s.funcmapCache[name]
	if !ok {
		c = &cache{
			funcid2name: make(map[int]string),
			funcname2id: make(map[string]int),
		}
		db := s.findDB(name)
		if db == nil {
			// TODO error handling
			return nil
		}
		stmt := `
SELECT
	funcid, funcname
FROM
	funcmap
`
		rows, err := db.Query(stmt)
		if err != nil {
			// TODO error handling
			return nil
		}
		defer rows.Close()
		for rows.Next() {
			var funcid int
			var funcname string
			if err := rows.Scan(&funcid, &funcname); err != nil {
				log.Fatal(err)
			}
			c.funcid2name[funcid] = funcname
			c.funcname2id[funcname] = funcid
		}
		s.funcmapCache[name] = c
	}
	return c
}

func (s *Schwartz) SetVerbose(verbose bool) {
	// TODO implements
	s.Verbose = verbose
}

func canDo(t interface{}) {

}

// --------------------
// POSTING JOBS
// --------------------

func (s *Schwartz) Insert(job *Job) error {
	// TODO implements
	return nil
}

func (s *Schwartz) InsertFuncNameWithArgs(funcname string, args ...string) error {
	// TODO implements
	return nil
}

func (s *Schwartz) InsertJobs(jobs []*Job) error {
	// TODO implements
	return nil
}

func (s *Schwartz) SetPrioritize(prioritize int) {
	// TODO implements
	s.Prioritize = prioritize
}

func (s *Schwartz) SetFloor(floor int) {
	// TODO implements
	s.Floor = floor
}

func (s *Schwartz) SetBatchSize(batchSize int) {
	// TODO implements
	s.BatchSize = batchSize
}

func (s *Schwartz) SetStrictRemoveAbility(strictRemoveAbility bool) {
	// TODO implements
	s.StrictRemoveAbility = strictRemoveAbility
}

// --------------------
// WORKING
// --------------------

func (s *Schwartz) CanDo(ability string) bool {
	// TODO implements
	return false
}

func (s *Schwartz) WorkOnce() error {
	// TODO implements
	return nil
}

func (s *Schwartz) WorkUntilDone() error {
	// TODO implements
	return nil
}

func (s *Schwartz) Work(delay *time.Duration) error {
	if delay == nil {
		*delay = time.Duration(5 * time.Second)
	}
	// TODO implements

	jobs := make(chan *Job)

	for job := range jobs {
		go func(job *Job) {
			trackJob(job)
			workers[job.FuncName].Work(job)
			untrackJob(job)
		}(job)
	}

	return nil
}

func (s *Schwartz) WorkOn(handle string) error {
	// TODO implements
	return nil
}

func (s *Schwartz) GrabAndWorkOn(handle string) error {
	// TODO implements
	return nil
}

func (s *Schwartz) FindJobForWorkers(abilities []string) *Job {
	// TODO implements
	return nil
}

func (s *Schwartz) FindJobWithCoalescingValue(ability string, coval interface{}) *Job {
	// TODO implements
	return nil
}

func (s *Schwartz) FindJobwithCoalescingPrefix(ability string, coval interface{}) *Job {
	// TODO implements
	return nil
}

func (s *Schwartz) GetServerTime(driver string) time.Time {
	// TODO implements
	return time.Now()
}
