package beanpod

import (
	"github.com/kr/beanstalk"
	"time"
)

type Conn struct {
	*beanstalk.Conn
}

func Dial(addr string) (*Conn, error) {
	c, err := beanstalk.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Conn{c}, nil
}

// Reserve and return a job from one of the tubes. If no job is available before time timeout has passed, Reserve returns a ConnError recording ErrTimeout.
func (c *Conn) Reserve(timeout time.Duration, tubes ...string) (*Job, error) {
	if len(tubes) == 0 {
		tubes = []string{"default"}
	}
	ts := beanstalk.NewTubeSet(c.Conn, tubes...)
	jid, body, err := ts.Reserve(timeout)
	if err != nil {
		return nil, unwrap(err)
	}
	return &Job{id: jid, body: body, conn: c.Conn}, nil
}

// Put a job into a tube with priority pri and TTR ttr, and returns the id of the newly-created job. If delay is nonzero, the server will wait the given amount of time after returning to the client and before putting the job into the ready queue.
func (c *Conn) Put(tube string, body []byte, pri uint32, delay, ttr time.Duration) (JobID, error) {
	t := &beanstalk.Tube{c.Conn, tube}
	id, err := t.Put(body, pri, delay, ttr)
	if err != nil {
		return 0, unwrap(err)
	}
	return JobID(id), nil
}

// Get the statistical information about the server. 
func (c *Conn) Stats() (*Stats, error) {
	m, err := c.Conn.Stats()
	if err != nil {
		return nil, unwrap(err)
	}
	return &Stats{m}, nil
}

// Get the statistical information about a tube. 
func (c *Conn) StatsTube(tube string) (*TubeStats, error) {
	t := &beanstalk.Tube{c.Conn, tube}
	s, err := t.Stats()
	if err != nil {
		return nil, unwrap(err)
	}
	return &TubeStats{s}, nil
}

// Get the statistical information about a job. 
func (c *Conn) StatsJob(id JobID) (*JobStats, error) {
	m, err := c.Conn.StatsJob(uint64(id))
	if err != nil {
		return nil, unwrap(err)
	}
	return &JobStats{m}, nil
}

// Take up to bound jobs from the holding area and moves them into the ready queue, then returns the number of jobs moved. Jobs will be taken in the order in which they were last buried.
func (c *Conn) Kick(tube string, bound int) (int, error) {
	t := &beanstalk.Tube{c.Conn, tube}
	n, err := t.Kick(bound)
	if err != nil {
		return 0, unwrap(err)
	}
	return n, nil
}

// Delay any new job being reserved from the tube for a given time. 
func (c *Conn) Pause(tube string, dur time.Duration) error {
	t := &beanstalk.Tube{c.Conn, tube}
	return unwrap(t.Pause(dur))
}

// Get a copy of the job in the holding area that would be kicked next by Kick.
func (c *Conn) PeekBuried(tube string) (*Job, error) {
	t := &beanstalk.Tube{c.Conn, tube}
	jid, body, err := t.PeekBuried()
	if err != nil {
		return nil, unwrap(err)
	}
	return &Job{id: jid, body: body, conn: c.Conn}, nil
}

// Get a copy of the delayed job that is next to be put in t's ready queue.
func (c *Conn) PeekDelayed(tube string) (*Job, error) {
	t := &beanstalk.Tube{c.Conn, tube}
	jid, body, err := t.PeekDelayed()
	if err != nil {
		return nil, unwrap(err)
	}
	return &Job{id: jid, body: body, conn: c.Conn}, nil
}

// Get a copy of the job at the front of t's ready queue.
func (c *Conn) PeekReady(tube string) (*Job, error) {
	t := &beanstalk.Tube{c.Conn, tube}
	jid, body, err := t.PeekReady()
	if err != nil {
		return nil, unwrap(err)
	}
	return &Job{id: jid, body: body, conn: c.Conn}, nil
}
