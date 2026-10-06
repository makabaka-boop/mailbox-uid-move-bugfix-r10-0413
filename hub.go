package main

import "sync"

type EventType int

const (
	EventAppend EventType = iota
	EventStore
	EventExpunge
	EventMoveIn
	// EventMoveOut is a source mailbox losing one whole UID MOVE batch in a
	// single revision. Kept last so existing constant values stay stable.
	EventMoveOut
)

// Event is one committed mailbox revision. It is rendered independently for
// each selected session, but publication is serialized by MailboxHub.mu.
type Event struct {
	Type     EventType
	Mailbox  string
	Revision int64

	// Append
	Exists      int
	UIDValidity int64
	UID         int64

	// Store
	Messages []MessageInfo

	// Expunge. SeqNums are sequence numbers before deletion, in ascending order.
	UIDs    []int64
	SeqNums []int
}

type MailboxHub struct {
	name        string
	mu          sync.Mutex
	subscribers map[*Session]struct{}
}

func (h *MailboxHub) subscribeLocked(s *Session) {
	h.subscribers[s] = struct{}{}
}

func (h *MailboxHub) unsubscribe(s *Session) {
	h.mu.Lock()
	delete(h.subscribers, s)
	h.mu.Unlock()
}

func (h *MailboxHub) publishLocked(ev Event, except *Session) {
	frame := renderEvent(ev)
	for sess := range h.subscribers {
		if sess == except {
			continue
		}
		// Events and command replies use the same per-connection channel.
		// A full channel means the client cannot preserve this revision
		// stream, so disconnect rather than block later mailbox revisions.
		if !sess.enqueue(frame) {
			go sess.Close()
		}
	}
}
