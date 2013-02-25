package beanpod

import (
	"github.com/kr/beanstalk"
	"time"
)

// Unique ID of a job assigned by the server.
type JobID uint64

// Job priority: lower values are more urgent
type JobPriority uint32

type Job struct {
	conn *beanstalk.Conn
	id   uint64
	body []byte
}

func (j *Job) ID() JobID {
	return JobID(j.id)
}

func (j *Job) Body() []byte {
	return j.body
}

// Remove the job from the server entirely. It is normally used by the client when the job has successfully run to completion.
func (j *Job) Delete() error {
	return unwrap(j.conn.Delete(j.id))
}

// Get the statistical information about the specified job if it exists.
func (j *Job) Stats() (*JobStats, error) {
	m, err := j.conn.StatsJob(j.id)
	if err != nil {
		return nil, unwrap(err)
	}
	return &JobStats{m}, nil
}

// Put the job into the "buried" state. Buried jobs are put into a FIFO linked list and will not be touched by the server again until a client kicks them.
func (j *Job) Bury(pri JobPriority) error {
	return unwrap(j.conn.Bury(j.id, uint32(pri)))
}

// Put the reserved job back into the ready queue (and marks its state as ready) to be run by any client. It is normally used when the job fails because of a transitory error.
func (j *Job) Release(pri JobPriority, delay time.Duration) error {
	return unwrap(j.conn.Release(j.id, uint32(pri), delay))
}

// Request more time to work on the job. This is useful for jobs that potentially take a long time, but you still want the benefits of a TTR pulling a job away from an unresponsive worker. A worker may periodically tell the server that it's still alive and processing a job (e.g. it may do this on DEADLINE_SOON).
func (j *Job) Touch() error {
	return unwrap(j.conn.Touch(j.id))
}
