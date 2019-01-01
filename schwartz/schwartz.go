package schwartz

import (
	"database/sql"
	"errors"
	"log"
	"time"
	"context"
	"math/rand"
	"sync"
	"net"
	"sync/atomic"
)

const (
	RetryDefault     = 30
	FindJobBatchSize = 50
)

func NewSchwartz(databases Databases) *Schwartz {
	var schwartz *Schwartz
	for k, v := range databases {
		schwartz.databases[k] = &DB{DB: v}
	}
	schwartz.RetrySeconds = RetryDefault
	schwartz.BatchSize = FindJobBatchSize
	// TODO implements
	return schwartz
}

type DatabaseName = string

type Databases = map[DatabaseName] *sql.DB
type databases = map[DatabaseName] *DB

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

	Verbose               bool
	Prioritize            bool
	Floor                 int
	BatchSize             int
	DriverCacheExpiration time.Duration
	RetrySeconds          time.Duration
	StrictRemoveAbility   bool

	databases databases
	funcmapCache map[string]*cache

	workers Workers

	repository Repository


	inShutdown        int32 // 0 or 1. accessed atomically (non-zero means we're in Shutdown)
	mu                sync.Mutex
	activeJob        map[*Job]struct{}
	activeJobWg      sync.WaitGroup
	doneChan          chan struct{}
	onShutdown        []func()
}

func (s *Schwartz) shuttingDown() bool {
	return atomic.LoadInt32(&s.inShutdown) != 0
}

func (s *Schwartz) trackJob(job *Job, add bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.activeJob == nil {
		s.activeJob = make(map[*Job]struct{})
	}
	if add {
		s.activeJob[job] = struct{}{}
		s.activeJobWg.Add(1)
	} else {
		delete(s.activeJob, job)
		s.activeJobWg.Done()
	}
}

func (s *Schwartz) getDoneChan() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.getDoneChanLocked()
}

func (s *Schwartz) getDoneChanLocked() chan struct{} {
	if s.doneChan == nil {
		s.doneChan = make(chan struct{})
	}
	return s.doneChan
}

func (s *Schwartz) closeDoneChanLocked() {
	ch := s.getDoneChanLocked()
	select {
	case <-ch:
		// Already closed. Don't close again.
	default:
		// Safe to close here. We're the only closer, guarded
		// by s.mu.
		close(ch)
	}
}

func (s *Schwartz) RegisterOnShutdown(f func()) {
	s.mu.Lock()
	s.onShutdown = append(s.onShutdown, f)
	s.mu.Unlock()
}

func (s *Schwartz) Shutdown(ctx context.Context) error {
	atomic.StoreInt32(&s.inShutdown, 1)

	s.mu.Lock()
	s.closeDoneChanLocked()
	for _, f := range s.onShutdown {
		go f()
	}
	s.mu.Unlock()

	finished := make(chan struct{}, 1)
	go func() {
		s.activeJobWg.Wait()
		finished <- struct{}{}
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-finished:
		return nil
	}
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

func (s *Schwartz) funcNameToID(name DatabaseName, funcname string) int {
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


func (s *Schwartz) insertJobToDriver(name DatabaseName, job *Job) error {
	job.FuncID = s.funcNameToID(name, job.FuncName)
	currentTime, err := s.repository.GetServerTime(s.Databases[name])
	if err != nil {
		// TODO
		return err
	}
	job.InsertTime = currentTime.Unix()
	err = s.repository.AddJob(s.Databases[name], job)
	if err != nil {
		// TODO
		return err
	}
	if job.ID > 0 {

	}
}

func (s *Schwartz) Insert(job *Job) error {
	// TODO implements
	for dbName, db := range s.Databases {
		if s.isDatabaseDead(dbName) {
			// TODO
			continue
		}
		s.insertJobToDriver(dbName, job)
		return nil
	}
	return errors.New("can not insert")
}

func (s *Schwartz) InsertFuncNameWithArgs(funcname string, arg []byte) error {
	// TODO implements
	var job Job
	job.FuncName = funcname
	job.Arg = arg
	job.
	return nil
}

func (s *Schwartz) InsertJobs(jobs []*Job) error {
	// TODO implements
	for dbName, db := range s.Databases {
		if s.isDatabaseDead(dbName) {
			// TODO
			continue
		}

		err := WithTransaction(db, func(tx Transaction) error {
			for _, job := range jobs {
				err := s.insertJobToDriver(dbName, job)
				if err != nil {
					return err
				}
			}
		})
		if err != nil {
			// TODO
		}

	}
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


func (s *Schwartz) Work(delay *time.Duration, quit <-chan bool) error {
	if delay == nil {
		*delay = time.Duration(5 * time.Second)
	}

	JobFinder{}

	jobs, err := s.poll(*delay, quit)
	if err != nil {
		return err
	}
	for job := range jobs {
		go func(job *Job) {
			s.trackJob(job, true)
			defer s.trackJob(job, false)
			if err := s.workers[job.FuncName].Work(job); err != nil {
				if err := job.Failed(); err != nil {
					// TODO
				}
				return
			}
			if err := job.Completed(); err != nil {
				// TODO
			}
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
	var grabbedJobs []*Job
	for dbName, db := range s.Databases {
		if s.isDatabaseDead(dbName) {
			continue
		}
		ids := s.funcNamesToIDs(dbName, workers)
		jobs, err := s.repository.FindJobsByFuncIDsOrderByJobID(db, ids, Floor(s.Floor), Prioritize(s.Prioritize))
		if err != nil {
			// TODO error handling
			continue
		}
		grabbedJobs = append(grabbedJobs, s.graJobs(db, jobs))
	}
	return grabbedJobs
}

func (s *Schwartz) graJobs(db *sql.DB, jobs []*Job) []*Job {
	shuffle(jobs)
	var grabbedJobs []*Job
	for _, job := range jobs {
		job.FuncName = s.funcIDToName(db, job.FuncID)
		oldGrabbedUntil := job.GrabbedUntil
		if err := s.repository.UpdateJob(db, job, oldGrabbedUntil); err != nil {
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