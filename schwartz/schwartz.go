package schwartz

import (
	"database/sql"
	"log"
	"time"

	"github.com/vvatanabe/GoSchwartz/schwartz/jobhandle"
)

const (
	RetryDefault     = 30
	FindJobBatchSize = 50
)

func NewSchwartz(db *sql.DB) *Schwartz {
	var schwartz *Schwartz
	schwartz.DB = db
	schwartz.RetrySeconds = RetryDefault
	schwartz.BatchSize = FindJobBatchSize
	// TODO implements
	return schwartz
}

type Schwartz struct {
	DB                    *sql.DB
	Verbose               bool
	Prioritize            int
	Floor                 int
	BatchSize             int
	DriverCacheExpiration time.Duration
	RetrySeconds          time.Duration
	StrictRemoveAbility   bool

	funcmapCache *funcmapCache
}

type funcmapCache struct {
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

	Handle *jobhandle.JobHandle
}

func (s *Schwartz) isDatabaseDead() bool {
	if err := s.DB.Ping(); err != nil {
		return true
	}
	return false
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
	stmt := `
SELECT
	jobid, funcid, arg, uniqkey, insert_time, run_after, grabbed_until, priority, coalesce
FROM
  job
WHERE
  jobid = ?
`
	var job Job
	err := s.DB.QueryRow(stmt, jobID).Scan(job, &job.JobID, &job.FuncID, &job.Arg, &job.UniqKey, &job.InsertTime,
		&job.RunAfter, &job.GrabbedUntil, &job.Priority, &job.Coalesce)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil
		} else {
			return nil
		}
	}

	job.Handle = handleFromString(jobID)
	job.FuncName = s.funcIDToName(job.FuncID)
	return &job
}

func handleFromString(jobID int) *jobhandle.JobHandle {
	return jobhandle.NewFromString(jobID)
}

func (s *Schwartz) funcIDToName(funcID int) string {
	return s.funcmapCache.funcid2name[funcID]

}

func (s *Schwartz) funcMapCache() {
	if s.funcmapCache == nil {
		s.funcmapCache = &funcmapCache{
			funcid2name: make(map[int]string),
			funcname2id: make(map[string]int),
		}
		stmt := `
SELECT
	funcid, funcname
FROM
	funcmap
`
		rows, err := s.DB.Query(stmt)
		if err != nil {
			// TODO error handling
			log.Fatalln(err.Error())
		}
		defer rows.Close()

		for rows.Next() {
			var funcid int
			var funcname string
			if err := rows.Scan(&funcid, &funcname); err != nil {
				log.Fatal(err)
			}
			s.funcmapCache.funcid2name[funcid] = funcname
			s.funcmapCache.funcname2id[funcname] = funcid
		}
	}

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
