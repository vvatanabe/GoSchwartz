package schwartz

import (
	"log"
)

type jobFinder interface {
	findJobsForWorkers(names []string) []*Job
}

type cache struct {
	funcid2name map[int64]string
	funcname2id map[string]int64
}

type finder struct {
	floor             int
	prioritize        bool
	databases         databases
	funcMapCache      map[string]*cache
	jobRepository     jobRepository
	funcMapRepository funcMapRepository
}

func (f *finder) findJobsForWorkers(workers []string) []*Job {
	// TODO implements
	var grabbedJobs []*Job
	for dbName, db := range f.databases {
		if db.isDead() {
			continue
		}
		ids := f.funcNamesToIDs(dbName, workers)
		jobs, err := f.jobRepository.FindJobsByFuncIDsOrderByJobID(db, ids, Floor(f.floor), Prioritize(f.prioritize))
		if err != nil {
			// TODO error handling
			continue
		}
		grabbedJobs = append(grabbedJobs, f.grabJobs(db, jobs)...)
	}
	return grabbedJobs
}

func (f *finder) grabJobs(db *DB, jobs []*Job) []*Job {
	shuffle(jobs)
	var grabbedJobs []*Job
	for _, job := range jobs {
		job.FuncName = f.funcIDToName(db, job.FuncID)
		oldGrabbedUntil := job.GrabbedUntil
		if err := f.jobRepository.UpdateJob(db, job, oldGrabbedUntil); err != nil {
			continue
		}
		grabbedJobs = append(grabbedJobs, job)
	}
	return grabbedJobs
}

func (f *finder) funcIDToName(name DatabaseName, funcID int) string {
	cache := f.getFuncMapCache(name)
	return cache.funcid2name[funcID]
}

func (f *finder) funcNameToID(name DatabaseName, funcname string) int {
	cache := f.getFuncMapCache(name)
	return cache.funcname2id[funcname]
}

func (f *finder) funcNamesToIDs(name DatabaseName, funcnames []string) []int {
	cache := f.getFuncMapCache(name)
	var ids []int
	for _, name := range funcnames {
		id := cache.funcname2id[name]
		ids = append(ids, id)
	}
	return ids
}

func (f *finder) getFuncMapCache(name DatabaseName) *cache {
	c, ok := f.funcMapCache[name]
	if !ok {
		c = &cache{
			funcid2name: make(map[int64]string),
			funcname2id: make(map[string]int64),
		}
		db, ok := f.databases[name]
		if !ok {
			return nil
		}
		fms, err := f.funcMapRepository.FindFuncMaps(db)
		if err != nil {
			// TODO
			log.Fatalln(err)
		}
		for _, fm := range fms {
			c.funcid2name[fm.ID] = fm.Name
			c.funcname2id[fm.Name] = fm.ID
		}
		f.FuncmapCache[name] = c
	}
	return c
}
