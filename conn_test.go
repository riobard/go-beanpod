package beanpod

import (
	"log"
	"testing"
)

func TestConn(t *testing.T) {
	c, err := Dial("localhost:11300")
	if err != nil {
		t.Fatal(err)
	}
	st, err := c.Stats()
	if err != nil {
		t.Fatal(err)
	}
	log.Printf("%s", st)

	log.Printf("urgent-jobs = %d", st.UrgentJobs())
	log.Printf("ready-jobs = %d", st.ReadyJobs())
	log.Printf("id = %s", st.Id())
	log.Printf("hostname = %s", st.Hostname())

	j, err := c.Reserve(0)
	if err != nil {
		t.Fatal(err)
	}
	s, err := j.Stats()
	if err != nil {
		t.Fatal(err)
	}
	log.Printf("%s", s)

	log.Printf("id = %v", s.Id())
	log.Printf("tube = %v", s.Tube())
	log.Printf("state = %v", s.State())
	log.Printf("pri = %v", s.Priority())
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
