package schwartz

import "time"

type jobPoller struct {
	finder jobFinder
}

func (p *jobPoller) poll(workers []string, quit <-chan bool, interval time.Duration) <-chan *Job {

	jobs := make(chan *Job)

	go func() {
		for {
			select {
			case <-quit:
				return
			default:
				for _, job := range p.finder.findJobsForWorkers(workers) {
					jobs <- job
				}
				if interval > 0 {
					time.Sleep(interval)
				}
			}
		}
	}()

	return jobs
}
