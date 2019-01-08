package schwartz

import (
	"errors"
	"time"
)

type job struct {
	ID           int
	FuncID       int
	Arg          []byte
	UniqKey      string
	InsertTime   time.Time
	RunAfter     time.Time
	GrabbedUntil time.Time
	Priority     int
	Coalesce     string
}

type Job struct {
	ID           int64
	FuncID       int
	FuncName     string
	Arg          []byte
	UniqKey      string
	InsertTime   int64
	RunAfter     time.Time
	GrabbedUntil time.Time
	Priority     int
	Coalesce     string

	failures []*FailureLog

	finished   bool
	exitStatus int
	handler    *JobHandle
}

func (j *Job) FailuresSize() int {
	return len(j.failures)
}

func (j *Job) Completed() error {
	if j.finished {
		return errors.New("already finished")
	}
	err := j.handler.Remove(j.ID)
	if err != nil {
		return err
	}
	j.finished = true
}

func (j *Job) Failed() error {
	if j.finished {
		return errors.New("already finished")
	}
	err := j.handler.Remove(j.ID)
	if err != nil {
		return err
	}
	j.finished = true
}

// test4
// test5
// test6