package beanpod

import (
	"github.com/kr/beanstalk"
	"time"
)

// Unique ID of a job assigned by the server.
type JobID uint64

// Job priority: lower values are more urgent
type JobPriority uint32

// Pre-defined job priority
const (
	PRI_URGENT JobPriority = 0
	PRI_HIGH               = 0x40000000
	PRI_NORMAL             = 0x80000000 // default job priority unless specified otherwise
	PRI_LOW                = 0xFFFFFFFF
)

// Pre-defined job time-to-run
const (
	TTR_NORMAL = 3 * time.Minute
)

// Beanstalkd client
type Client struct {
	addr string
	*beanstalk.Conn
}

// Make a beanstalk client to a server address (without connecting)
func New(addr string) *Client {
	return &Client{addr: addr}
}

// Connect to the server
func (c *Client) Connect() (err error) {
	if c.Conn != nil {
		return nil
	}
	c.Conn, err = beanstalk.Dial("tcp", c.addr)
	return err
}

func (c *Client) Close() error {
	if c.Conn == nil {
		return nil
	}
	return c.Conn.Close()
}

// Reserve and return a job from one of the tubes. If no job is available before time timeout has passed, Reserve returns a ConnError recording ErrTimeout.
func (c *Client) Reserve(timeout time.Duration, tubes ...string) (JobID, []byte, error) {
	err := c.Connect()
	if err != nil {
		return 0, nil, err
	}
	if len(tubes) == 0 {
		tubes = []string{"default"}
	}
	ts := beanstalk.NewTubeSet(c.Conn, tubes...)
	jid, body, err := ts.Reserve(timeout)
	if err != nil {
		return 0, nil, unwrap(err)
	}
	return JobID(jid), body, nil
}

// Put a job into a tube with priority pri and TTR ttr, and returns the id of the newly-created job. If delay is nonzero, the server will wait the given amount of time after returning to the client and before putting the job into the ready queue.
func (c *Client) Put(tube string, body []byte, pri uint32, delay, ttr time.Duration) (JobID, error) {
	err := c.Connect()
	if err != nil {
		return 0, err
	}
	t := &beanstalk.Tube{c.Conn, tube}
	id, err := t.Put(body, pri, delay, ttr)
	if err != nil {
		return 0, unwrap(err)
	}
	return JobID(id), nil
}

// Put a job with normal priority, no delay, and 180 seconds TTR
func (c *Client) PutDefault(tube string, body []byte) (JobID, error) {
	err := c.Connect()
	if err != nil {
		return 0, err
	}
	return c.Put(tube, body, uint32(PRI_NORMAL), 0, TTR_NORMAL)
}

// Get the statistical information about the server.
func (c *Client) Stats() (*Stats, error) {
	err := c.Connect()
	if err != nil {
		return nil, err
	}
	m, err := c.Conn.Stats()
	if err != nil {
		return nil, unwrap(err)
	}
	return &Stats{m}, nil
}

// Get the statistical information about a tube.
func (c *Client) StatsTube(tube string) (*TubeStats, error) {
	err := c.Connect()
	if err != nil {
		return nil, err
	}
	t := &beanstalk.Tube{c.Conn, tube}
	s, err := t.Stats()
	if err != nil {
		return nil, unwrap(err)
	}
	return &TubeStats{s}, nil
}

// Get the statistical information about a job.
func (c *Client) StatsJob(id JobID) (*JobStats, error) {
	err := c.Connect()
	if err != nil {
		return nil, err
	}
	m, err := c.Conn.StatsJob(uint64(id))
	if err != nil {
		return nil, unwrap(err)
	}
	return &JobStats{m}, nil
}

// Take up to bound jobs from the holding area and moves them into the ready queue, then returns the number of jobs moved. Jobs will be taken in the order in which they were last buried.
func (c *Client) Kick(tube string, bound int) (int, error) {
	err := c.Connect()
	if err != nil {
		return 0, err
	}
	t := &beanstalk.Tube{c.Conn, tube}
	n, err := t.Kick(bound)
	if err != nil {
		return 0, unwrap(err)
	}
	return n, nil
}

// Delay any new job being reserved from the tube for a given time.
func (c *Client) Pause(tube string, dur time.Duration) error {
	err := c.Connect()
	if err != nil {
		return err
	}
	t := &beanstalk.Tube{c.Conn, tube}
	return unwrap(t.Pause(dur))
}

// Get a copy of the job in the holding area that would be kicked next by Kick.
func (c *Client) PeekBuried(tube string) (JobID, []byte, error) {
	err := c.Connect()
	if err != nil {
		return 0, nil, err
	}
	t := &beanstalk.Tube{c.Conn, tube}
	jid, body, err := t.PeekBuried()
	if err != nil {
		return 0, nil, unwrap(err)
	}
	return JobID(jid), body, nil
}

// Get a copy of the delayed job that is next to be put in t's ready queue.
func (c *Client) PeekDelayed(tube string) (JobID, []byte, error) {
	err := c.Connect()
	if err != nil {
		return 0, nil, err
	}
	t := &beanstalk.Tube{c.Conn, tube}
	jid, body, err := t.PeekDelayed()
	if err != nil {
		return 0, nil, unwrap(err)
	}
	return JobID(jid), body, nil
}

// Get a copy of the job at the front of t's ready queue.
func (c *Client) PeekReady(tube string) (JobID, []byte, error) {
	err := c.Connect()
	if err != nil {
		return 0, nil, err
	}
	t := &beanstalk.Tube{c.Conn, tube}
	jid, body, err := t.PeekReady()
	if err != nil {
		return 0, nil, unwrap(err)
	}
	return JobID(jid), body, nil
}

// Remove the job from the server entirely. It is normally used by the client when the job has successfully run to completion.
func (c *Client) Delete(id JobID) error {
	err := c.Connect()
	if err != nil {
		return err
	}
	return unwrap(c.Conn.Delete(uint64(id)))
}

// Put the job into the "buried" state. Buried jobs are put into a FIFO linked list and will not be touched by the server again until a client kicks them.
func (c *Client) Bury(id JobID, pri JobPriority) error {
	err := c.Connect()
	if err != nil {
		return err
	}
	return unwrap(c.Conn.Bury(uint64(id), uint32(pri)))
}

// Put the reserved job back into the ready queue (and marks its state as ready) to be run by any client. It is normally used when the job fails because of a transitory error.
func (c *Client) Release(id JobID, pri JobPriority, delay time.Duration) error {
	err := c.Connect()
	if err != nil {
		return err
	}
	return unwrap(c.Conn.Release(uint64(id), uint32(pri), delay))
}

// Request more time to work on the job. This is useful for jobs that potentially take a long time, but you still want the benefits of a TTR pulling a job away from an unresponsive worker. A worker may periodically tell the server that it's still alive and processing a job (e.g. it may do this on DEADLINE_SOON).
func (c *Client) Touch(id JobID) error {
	err := c.Connect()
	if err != nil {
		return err
	}
	return unwrap(c.Conn.Touch(uint64(id)))
}
