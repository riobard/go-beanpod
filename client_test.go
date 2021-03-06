package beanpod

import (
	"log"
	"testing"
)

func TestConn(t *testing.T) {
	c := New("localhost:11300")
	st, err := c.Stats()
	if err != nil {
		t.Fatal(err)
	}
	log.Printf("%s", st)

	log.Printf("urgent-jobs = %d", st.UrgentJobs())
	log.Printf("ready-jobs = %d", st.ReadyJobs())
	log.Printf("id = %s", st.ID())
	log.Printf("hostname = %s", st.Hostname())
	log.Printf("utime = %v", st.RusageUtime())
	log.Printf("stime = %v", st.RusageStime())

	jid, _, err := c.Reserve(0)
	if err != nil {
		t.Fatal(err)
	}
	s, err := c.StatsJob(jid)
	if err != nil {
		t.Fatal(err)
	}
	log.Printf("%s", s)

	log.Printf("id = %v", s.ID())
	log.Printf("tube = %v", s.Tube())
	log.Printf("state = %v", s.State())
	log.Printf("pri = %v", s.Pri())
	log.Printf("age = %v", s.Age())
	log.Printf("time-left = %v", s.TimeLeft())
	log.Printf("delay = %v", s.Delay())
	log.Printf("TTR = %v", s.TTR())
	log.Printf("file = %v", s.File())
	log.Printf("reserves = %v", s.Reserves())
	log.Printf("timeouts = %v", s.Timeouts())
	log.Printf("releases = %v", s.Releases())
	log.Printf("buries = %v", s.Buries())
	log.Printf("kicks = %v", s.Kicks())
}
