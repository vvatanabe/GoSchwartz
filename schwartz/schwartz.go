package schwartz

import (
	"database/sql"
	"errors"
	"log"
	"time"
	"context"
	"math/rand"
	"sync"
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

type Databases = map[DatabaseName] *sql.DB

type WorkName = string

type Worker interface {
	Work(job *Job) error
	KeepExitStatusFor() time.Duration
	MaxRetries() int
	RetryDelay() time.Duration
	GrabFor() time.Duration
}

type Workers = map[WorkName]Worker

type Schwartz struct {
	Databases Databases
	Verbose               bool
	Prioritize            bool
	Floor                 int
	BatchSize             int
	DriverCacheExpiration time.Duration
	RetrySeconds          time.Duration
	StrictRemoveAbility   bool

	funcmapCache map[string]*cache

	workers Workers

	repository Repository
}

type cache struct {
	funcname2id map[string]int
	funcid2name map[int]string
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
	funcid       int
	runAfter     *runAfterTerm
	grabbedUntil *grabbedUntilTerm
	jobid        *jobIDTerm
}

type runAfterTerm struct {
	op    string
	value time.Duration
}

type grabbedUntilTerm struct {
	op    string
	value time.Duration
}

type jobIDTerm struct {
	op    string
	value int
}

func (s *Schwartz) ListJobs(
	funcname string,
	runAfter time.Duration,
	grabbedUntil time.Duration,
	coalesceOP string,
	coalesce string,
	wantHandle bool,
	jobID int,
	limit int) []*Job {
	var jobs []*Job
	// TODO implements
	var terms *terms
	if runAfter != 0 {
		terms.runAfter = &runAfterTerm{
			op:    "<=",
			value: runAfter,
		}
	}
	if grabbedUntil != 0 {
		terms.grabbedUntil = &grabbedUntilTerm{
			op:    "<=",
			value: grabbedUntil,
		}
	}
	if jobIDTerm != 0 {
		terms.jobid = &jobIDTerm{
			op:    "=",
			value: jobID,
		}
	}

	if funcname == "" {
		log.Fatalln("o funcname")
	}

	//limit :=

	for name, db := range s.Databases {
		if s.isDatabaseDead(name) { // TODO remove
			continue
		}

		db.Begin()
		tx, err := db.Begin()
		tx.
		var d *sql.DB
		terms.funcid = s.funcNameToID(name, funcname)

		if wantHandle {
			h := JobHandle{}
		}

	}

	return jobs
}


type Queryable interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	Prepare(query string) (*sql.Stmt, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}




var ErrNotFoundDB = errors.New("schwartz: Not found DB")

func (s *Schwartz) LookupJob(name DatabaseName, jobID int) (*Job, error) {
	db := s.findDB(name)
	if db == nil {
		return nil, ErrNotFoundDB
	}
	job, err := s.repository.FindJob(db, jobID)
	if err != nil {
		// TODO
		return nil, err
	}
	job.Handle = &JobHandle{name, jobID, s}
	job.FuncName = s.funcIDToName(name, job.FuncID)
	return job, nil
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

func (s *Schwartz) funcNameToID(name DatabaseName, funcname string) string {
	cache := s.funcMapCache(name)
	return cache.funcname2id[funcname]
}

func (s *Schwartz) funcNamesToIDs(name DatabaseName, funcnames []string) []int {
	cache := s.funcMapCache(name)
	var ids []int
	for _, name := range funcnames {
		id := cache.funcname2id[name]
		ids = append(ids, id)
	}
	return ids
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
		fms, err := s.repository.FindFuncMaps(db)
		if err != nil {
			// TODO
			log.Fatalln(err)
		}
		for _, fm := range fms {
			c.funcid2name[fm.ID] = fm.Name
			c.funcname2id[fm.Name] = fm.ID
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


func (s *Schwartz) innsertJobToDriver(name DatabaseName, job *Job) error {
	job.FuncID = s.funcNameToID(name, job.FuncName)
	job.InsertTime = time.Now().Unix()

	s.repository.AddJob(s.Databases[name], job)

}

func (s *Schwartz) Insert(job *Job) error {
	// TODO implements
	for k, v := range s.Databases {

	}
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

func (s *Schwartz) CanDo(worker Worker) bool {
	if worker == nil {
		return false
	}
	if s.workers == nil {
		s.workers = make(Workers)
	}
	s.workers[worker.Name()] = worker
	return true
}

func (s *Schwartz) WorkOnce() error {
	// TODO implements
	return nil
}

func (s *Schwartz) WorkUntilDone() error {
	// TODO implements
	return nil
}


func (s *Schwartz) poll(interval time.Duration, quit <-chan bool) (<-chan *Job, error) {
	jobs := make(chan *Job)

	go func() {
		for {
			select {
			case <-quit:
				return
			default:
				jobs <- s.FindJobsForWorkers(s.workers)
				time.Sleep(interval)
				}
		}
	}()

	return jobs, nil
}

func (s *Schwartz) Work(delay *time.Duration, quit <-chan bool) error {
	if delay == nil {
		*delay = time.Duration(5 * time.Second)
	}
	// TODO implements

	jobs, err := s.poll(*delay, quit)
	if err != nil {
		return err
	}

	for job := range jobs {
		go func(job *Job) {
			s.trackJob(job)
			defer s.untrackJob(job)
			if err := s.workers[job.FuncName].Work(job); err != nil {
				s.Failed(job)
				return
			}
			s.Completed(job)
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

func (s *Schwartz) FindJobsForWorkers(workers []string) []*Job {
	// TODO implements

	type option struct {

	}


	if s.Prioritize {

	} else {

	}

	var grabbedJobs []*Job
	for dbName, db := range s.Databases {

		if s.isDatabaseDead(dbName) {
			continue
		}

		ids := s.funcNamesToIDs(dbName, workers)

		// SELECT *
		// FROM job
		// WHERE funcid=? AND run_after<=? AND grabbed_until<=? AND priority>=?
		// ORDER BY priority desc, jobid
		type terms struct {
			funcids []int
			runAfter time.Time
			grabbedUntil time.Time
			priority int
		}

		t := terms{
			funcids: ids,
			runAfter: now,
			grabbedUntil: now,
		}

		if s.Floor > 0 {
			t.priority = s.Floor
		}

		jobs, err := s.repository.FindJobsByFuncIDsOrderByJobID(db, ids)
		if err != nil {
			// TODO error handling
			continue
		}

		grabbedJobs = append(grabbedJobs, s.graJobs(jobs))
	}

	return grabbedJobs
}

func (s *Schwartz) graJobs(db *sql.DB, jobs []*Job) []*Job {
	shuffle(jobs)

	var grabbedJobs []*Job
	for _, job := range jobs {
		job.FuncName = s.funcIDToName(db, job.FuncID)
		oldGrabbedUntill := job.GrabbedUntil

		if err := s.repository.UpdateJob(db, job, oldGrabbedUntill); err != nil {
			continue
		}
		grabbedJobs = append(grabbedJobs, job)
	}
	return grabbedJobs
}

func (s *Schwartz) FindJobWithCoalescingValue(ability string, coval interface{}) *Job {
	// TODO implements
	return nil
}

func (s *Schwartz) FindJobWithCoalescingPrefix(ability string, coval interface{}) *Job {
	// TODO implements
	return nil
}

func shuffle(jobs []*Job) {
	n := len(jobs)
	for i := n - 1; i >= 0; i-- {
		j := rand.Intn(i + 1)
		jobs[i], jobs[j] = jobs[j], jobs[i]
	}
}