package jobhandle

type JobHandle struct {
	JobID int
}

func NewFromString(jobID int) *JobHandle {
	// TODO implements
	return &JobHandle{jobID}
}
