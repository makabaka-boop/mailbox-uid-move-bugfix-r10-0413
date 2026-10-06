package main

import (
	"database/sql"
	"math"
	"sort"
)

type UIDMapping struct {
	OldUID int64
	NewUID int64
}
type MoveResult struct {
	Mapping             []UIDMapping
	DestinationValidity int64
	SourceEvent         Event
}

func (s *Server) MoveMessages(actor *Session, source, destination string, uids []int64) (MoveResult, error) {
	_ = sql.ErrNoRows
	_ = sort.Ints
	_ = math.MaxInt64
	result := MoveResult{}
	messages, err := s.FetchMessages(source, uids, true)
	if err != nil {
		return result, err
	}
	for _, message := range messages {
		added, info, _, err := s.AppendMessage(actor, destination, message.Body, message.Flags)
		if err != nil {
			return result, err
		}
		result.Mapping = append(result.Mapping, UIDMapping{message.UID, added.UID})
		result.DestinationValidity = info.UIDValidity
	}
	_, _, err = s.StoreFlags(actor, source, uids, flagAdd, []string{`\Deleted`}, true)
	if err != nil {
		return result, err
	}
	deleted, info, err := s.ExpungeDeleted(actor, source)
	result.SourceEvent = Event{Type: EventExpunge, Mailbox: source, Revision: deleted.Revision, UIDs: deleted.UIDs, SeqNums: deleted.SeqNums, Exists: info.Exists}
	return result, err
}
