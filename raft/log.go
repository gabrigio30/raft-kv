package raft

// LogEntry represents a single entry in the Raft log.
type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

// lastLogIndexAndTerm returns the index and term of the last log entry,
// or (0, 0) if the log is empty.
func lastLogIndexAndTerm(log []LogEntry) (int, int) {
	if len(log) == 0 {
		return 0, 0
	}
	last := log[len(log)-1]
	return last.Index, last.Term
}