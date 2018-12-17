package schwartz

import "time"

type Job struct {
	ID           int64
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
