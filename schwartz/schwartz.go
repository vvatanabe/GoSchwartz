package schwartz

import "time"

type Schwartz struct {
	Databases             *Databases
	Verbose               bool
	Prioritize            int
	Floor                 int
	BatchSize             int
	DriverCacheExpiration time.Duration
	RetrySeconds          time.Duration
	StrictRemoveAbility   bool
}

type Databases struct {
	DSN    string
	User   string
	Pass   string
	Driver string
}

type Job struct {
	// TODO implements
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
	return jobs
}

func (s *Schwartz) LookupJob(jobID int) *Job {
	// TODO implements
	return nil
}

func (s *Schwartz) SetVerbose(verbose bool) {
	// TODO implements
	s.Verbose = verbose
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
