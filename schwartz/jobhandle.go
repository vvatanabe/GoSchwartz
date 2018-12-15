package schwartz

type JobHandle struct {
	Name     DatabaseName
	JobID    int
	schwartz *Schwartz
}

func NewJobHandle(name string, jobID int) *JobHandle {
	// TODO implements
	return &JobHandle{Name: name, JobID: jobID}
}
