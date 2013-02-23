package beanpod

import (
	"strconv"
	"time"
)

const (
	S_READY    = "ready"
	S_DELAYED  = "delayed"
	S_RESERVED = "reserved"
	S_BURIED   = "buried"
)

// Statistical information about the system as a whole.
type Stats struct {
	m map[string]string
}

// Number of ready jobs with priority < 1024.
func (s *Stats) UrgentJobs() int {
	n, _ := strconv.ParseUint(s.m["current-jobs-urgent "], 10, 64)
	return int(n)
}

// Number of jobs in the ready queue.
func (s *Stats) ReadyJobs() int {
	n, _ := strconv.ParseUint(s.m["current-jobs-ready"], 10, 64)
	return int(n)
}

// Number of jobs reserved by all clients.
func (s *Stats) ReservedJobs() int {
	n, _ := strconv.ParseUint(s.m["current-jobs-reserved"], 10, 64)
	return int(n)
}

// Number of delayed jobs.
func (s *Stats) DelayedJobs() int {
	n, _ := strconv.ParseUint(s.m["current-jobs-delayed"], 10, 64)
	return int(n)
}

// Number of buried jobs.
func (s *Stats) BuriedJobs() int {
	n, _ := strconv.ParseUint(s.m["current-jobs-buried"], 10, 64)
	return int(n)
}

// Cumulative number of put commands.
func (s *Stats) PutCmds() int {
	n, _ := strconv.ParseUint(s.m["cmd-put"], 10, 64)
	return int(n)
}

// Cumulative number of peek commands.
func (s *Stats) PeekCmds() int {
	n, _ := strconv.ParseUint(s.m["cmd-peek"], 10, 64)
	return int(n)
}

// Cumulative number of peek-ready commands.
func (s *Stats) PeekReadyCmds() int {
	n, _ := strconv.ParseUint(s.m["cmd-peek-ready"], 10, 64)
	return int(n)
}

// Cumulative number of peek-delayed commands.
func (s *Stats) PeekDelayedCmds() int {
	n, _ := strconv.ParseUint(s.m["cmd-peek-delayed"], 10, 64)
	return int(n)
}

// Cumulative number of peek-buried commands.
func (s *Stats) PeekBuriedCmds() int {
	n, _ := strconv.ParseUint(s.m["cmd-peek-buried"], 10, 64)
	return int(n)
}

// Cumulative number of reserve commands.
func (s *Stats) ReserveCmds() int {
	n, _ := strconv.ParseUint(s.m["cmd-reserve"], 10, 64)
	return int(n)
}

// Cumulative number of use commands.
func (s *Stats) UseCmds() int {
	n, _ := strconv.ParseUint(s.m["cmd-use"], 10, 64)
	return int(n)
}

// Cumulative number of watch commands.
func (s *Stats) WatchCmds() int {
	n, _ := strconv.ParseUint(s.m["cmd-watch"], 10, 64)
	return int(n)
}

// Cumulative number of ignore commands.
func (s *Stats) IgnoreCmds() int {
	n, _ := strconv.ParseUint(s.m["cmd-ignore"], 10, 64)
	return int(n)
}

// Cumulative number of delete commands.
func (s *Stats) DeleteCmds() int {
	n, _ := strconv.ParseUint(s.m["cmd-delete"], 10, 64)
	return int(n)
}

// Cumulative number of release commands.
func (s *Stats) ReleaseCmds() int {
	n, _ := strconv.ParseUint(s.m["cmd-release"], 10, 64)
	return int(n)
}

// Cumulative number of bury commands.
func (s *Stats) BuryCmds() int {
	n, _ := strconv.ParseUint(s.m["cmd-bury"], 10, 64)
	return int(n)
}

// Cumulative number of kick commands.
func (s *Stats) KickCmds() int {
	n, _ := strconv.ParseUint(s.m["cmd-kick"], 10, 64)
	return int(n)
}

// Cumulative number of stats commands.
func (s *Stats) StatsCmds() int {
	n, _ := strconv.ParseUint(s.m["cmd-stats"], 10, 64)
	return int(n)
}

// Cumulative number of stats-job commands.
func (s *Stats) StatsJobCmds() int {
	n, _ := strconv.ParseUint(s.m["cmd-stats-job"], 10, 64)
	return int(n)
}

// Cumulative number of stats-tube commands.
func (s *Stats) StatsTubeCmds() int {
	n, _ := strconv.ParseUint(s.m["cmd-stats-tube"], 10, 64)
	return int(n)
}

// Cumulative number of list-tubes commands.
func (s *Stats) ListTubesCmds() int {
	n, _ := strconv.ParseUint(s.m["cmd-list-tubes"], 10, 64)
	return int(n)
}

// Cumulative number of list-tube-used commands.
func (s *Stats) ListTubeUsedCmds() int {
	n, _ := strconv.ParseUint(s.m["cmd-list-tube-used"], 10, 64)
	return int(n)
}

// Cumulative number of list-tubes-watched commands.
func (s *Stats) ListTubesWatchedCmds() int {
	n, _ := strconv.ParseUint(s.m["cmd-list-tubes-watched"], 10, 64)
	return int(n)
}

// Cumulative number of pause-tube commands
func (s *Stats) PauseTubeCmds() int {
	n, _ := strconv.ParseUint(s.m["cmd-pause-tube"], 10, 64)
	return int(n)
}

// Cumulative count of times a job has timed out.
func (s *Stats) JobTimeouts() int {
	n, _ := strconv.ParseUint(s.m["job-timeouts"], 10, 64)
	return int(n)
}

// Cumulative count of jobs created.
func (s *Stats) TotalJobs() int {
	n, _ := strconv.ParseUint(s.m["total-jobs"], 10, 64)
	return int(n)
}

// Maximum number of bytes in a job.
func (s *Stats) MaxJobSize() int {
	n, _ := strconv.ParseUint(s.m["max-job-size"], 10, 64)
	return int(n)
}

// Number of currently-existing tubes.
func (s *Stats) CurrentTubes() int {
	n, _ := strconv.ParseUint(s.m["current-tubes"], 10, 64)
	return int(n)
}

// Number of currently open connections.
func (s *Stats) CurrentConnections() int {
	n, _ := strconv.ParseUint(s.m["current-connections"], 10, 64)
	return int(n)
}

// Number of open connections that have each issued at least one put command.
func (s *Stats) CurrentProducers() int {
	n, _ := strconv.ParseUint(s.m["current-producers"], 10, 64)
	return int(n)
}

// Number of open connections that have each issued at least one reserve command.
func (s *Stats) CurrentWorkers() int {
	n, _ := strconv.ParseUint(s.m["current-workers"], 10, 64)
	return int(n)
}

// Number of open connections that have issued a reserve command but not yet received a response.
func (s *Stats) CurrentWaiting() int {
	n, _ := strconv.ParseUint(s.m["current-waiting"], 10, 64)
	return int(n)
}

//  Cumulative count of connections.
func (s *Stats) TotalConnections() int {
	n, _ := strconv.ParseUint(s.m["total-connections"], 10, 64)
	return int(n)
}

// Process id of the server.
func (s *Stats) PID() int {
	n, _ := strconv.ParseUint(s.m["pid"], 10, 64)
	return int(n)
}

// Version string of the server.
func (s *Stats) Version() string {
	return s.m["version"]
}

// Cumulative user CPU time of this process in seconds and microseconds.
func (s *Stats) RusageUtime() time.Duration {
	n, _ := strconv.ParseUint(s.m["rusage-utime"], 10, 64)
	return time.Duration(n) * time.Second
}

// Cumulative system CPU time of this process in seconds and microseconds.
func (s *Stats) RusageStime() time.Duration {
	n, _ := strconv.ParseUint(s.m["rusage-stime"], 10, 64)
	return time.Duration(n) * time.Second
}

// Number of seconds since this server process started running.
func (s *Stats) Uptime() time.Duration {
	n, _ := strconv.ParseUint(s.m["uptime"], 10, 64)
	return time.Duration(n) * time.Second
}

// Index of the oldest binlog file needed to store the current jobs.
func (s *Stats) BinlogOldestIndex() int {
	n, _ := strconv.ParseUint(s.m["binlog-oldest-index"], 10, 64)
	return int(n)
}

// Index of the current binlog file being written to. If binlog is not active this value will be 0.
func (s *Stats) BinlogCurrentIndex() int {
	n, _ := strconv.ParseUint(s.m["binlog-current-index"], 10, 64)
	return int(n)
}

// Maximum size in bytes a binlog file is allowed to get before a new binlog file is opened.
func (s *Stats) BinlogMaxSize() uint64 {
	n, _ := strconv.ParseUint(s.m["binlog-max-size"], 10, 64)
	return n
}

// Cumulative number of records written to the binlog.
func (s *Stats) BinlogRecordsWritten() int {
	n, _ := strconv.ParseUint(s.m["binlog-records-written"], 10, 64)
	return int(n)
}

// Cumulative number of records written as part of compaction.
func (s *Stats) BinlogRecordsMigrated() int {
	n, _ := strconv.ParseUint(s.m["binlog-records-migrated"], 10, 64)
	return int(n)
}

// A unique id for this server process. The id is generated on each startup and is always a random series of 8 bytes base16 encoded
func (s *Stats) ID() string {
	return s.m["id"]
}

// Hostname of the machine as determined by uname
func (s *Stats) Hostname() string {
	return s.m["hostname"]
}

// Statistical information about a tube. 
type TubeStats struct {
	m map[string]string
}

// Name of the tube.
func (s *TubeStats) Name() string {
	return s.m["name"]
}

// Number of ready jobs with priority < 1024 in this tube.
func (s *TubeStats) UrgentJobs() int {
	n, _ := strconv.ParseUint(s.m["current-jobs-urgent"], 10, 32)
	return int(n)
}

// Number of jobs in the ready queue in this tube.
func (s *TubeStats) ReadyJobs() int {
	n, _ := strconv.ParseUint(s.m["current-jobs-ready"], 10, 32)
	return int(n)
}

// Number of jobs reserved by all clients in this tube.
func (s *TubeStats) ReservedJobs() int {
	n, _ := strconv.ParseUint(s.m["current-jobs-reserved"], 10, 32)
	return int(n)
}

// Number of delayed jobs in this tube.
func (s *TubeStats) DelayedJobs() int {
	n, _ := strconv.ParseUint(s.m["current-jobs-delayed"], 10, 32)
	return int(n)
}

// Number of buried jobs in this tube.
func (s *TubeStats) BuriedJobs() int {
	n, _ := strconv.ParseUint(s.m["current-jobs-buried"], 10, 32)
	return int(n)
}

// Cumulative count of jobs created in this tube in the current beanstalkd process.
func (s *TubeStats) TotalJobs() int {
	n, _ := strconv.ParseUint(s.m["total-jobs"], 10, 32)
	return int(n)
}

// Number of open connections that are currently using this tube.
func (s *TubeStats) Using() int {
	n, _ := strconv.ParseUint(s.m["current-using"], 10, 32)
	return int(n)
}

// Number of open connections that have issued a reserve command while watching this tube but not yet received a response.
func (s *TubeStats) Waiting() int {
	n, _ := strconv.ParseUint(s.m["current-waiting"], 10, 32)
	return int(n)
}

// Number of open connections that are currently watching this tube.
func (s *TubeStats) Watching() int {
	n, _ := strconv.ParseUint(s.m["current-watching"], 10, 32)
	return int(n)
}

// Number of seconds the tube has been paused for.
func (s *TubeStats) Pause() time.Duration {
	n, _ := strconv.ParseUint(s.m["pause"], 10, 32)
	return time.Duration(n) * time.Second
}

// Cumulative number of delete commands for this tube
func (s *TubeStats) DeleteCmds() int {
	n, _ := strconv.ParseUint(s.m["cmd-delete"], 10, 32)
	return int(n)
}

// Cumulative number of pause-tube commands for this tube.
func (s *TubeStats) PauseTubeCmds() int {
	n, _ := strconv.ParseUint(s.m["cmd-pause-tube"], 10, 32)
	return int(n)
}

// Number of seconds until the tube is un-paused.
func (s *TubeStats) PauseTimeLeft() time.Duration {
	n, _ := strconv.ParseUint(s.m["pause-time-left"], 10, 32)
	return time.Duration(n) * time.Second
}

// Statistical information about a job.
type JobStats struct {
	m map[string]string
}

// Job ID.
func (s *JobStats) ID() JobID {
	n, _ := strconv.ParseUint(s.m["id"], 10, 64)
	return JobID(n)
}

// Name of the tube that contains this job.
func (s *JobStats) Tube() string {
	return s.m["tube"]
}

// Possible values: ready, delayed, reserved, or buried.
func (s *JobStats) State() string {
	return s.m["state"]
}

// Priority value set by the put, release, or bury commands.
func (s *JobStats) Pri() uint32 {
	n, _ := strconv.ParseUint(s.m["pri"], 10, 32)
	return uint32(n)
}

// Time in seconds since the put command that created this job.
func (s *JobStats) Age() time.Duration {
	n, _ := strconv.ParseUint(s.m["age"], 10, 64)
	return time.Duration(n) * time.Second
}

// Number of seconds left until the server puts this job into the ready queue. This number is only meaningful if the job is reserved or delayed. If the job is reserved and this amount of time elapses before its state changes, it is considered to have timed out.
func (s *JobStats) TimeLeft() time.Duration {
	n, _ := strconv.ParseUint(s.m["time-left"], 10, 64)
	return time.Duration(n) * time.Second
}

// Time in seconds that the job is delayed. 
func (s *JobStats) Delay() time.Duration {
	n, _ := strconv.ParseUint(s.m["delay"], 10, 64)
	return time.Duration(n) * time.Second
}

// Time to run in seconds. 
func (s *JobStats) TTR() time.Duration {
	n, _ := strconv.ParseUint(s.m["ttr"], 10, 64)
	return time.Duration(n) * time.Second
}

// Number of the earliest binlog file containing this job. If -b wasn't used, this will be 0.
func (s *JobStats) File() int {
	n, _ := strconv.ParseUint(s.m["file"], 10, 64)
	return int(n)
}

// Number of times this job has been reserved.
func (s *JobStats) Reserves() int {
	n, _ := strconv.ParseUint(s.m["reserves"], 10, 64)
	return int(n)
}

// Number of times this job has timed out during a reservation.
func (s *JobStats) Timeouts() int {
	n, _ := strconv.ParseUint(s.m["timeouts"], 10, 64)
	return int(n)
}

// Number of times a client has released this job from a reservation.
func (s *JobStats) Releases() int {
	n, _ := strconv.ParseUint(s.m["releases"], 10, 64)
	return int(n)
}

// Number of times this job has been buried.
func (s *JobStats) Buries() int {
	n, _ := strconv.ParseUint(s.m["buries"], 10, 64)
	return int(n)
}

// Number of times this job has been kicked.
func (s *JobStats) Kicks() int {
	n, _ := strconv.ParseUint(s.m["kicks"], 10, 64)
	return int(n)
}
