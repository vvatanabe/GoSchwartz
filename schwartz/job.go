package schwartz

import "time"

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

type (j *Job) GetFunc() {

}