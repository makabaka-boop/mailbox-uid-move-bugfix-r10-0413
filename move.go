package main

import (
	"database/sql"
	"errors"
	"fmt"
)

var (
	// ErrEmptyUIDSet is a command error: MOVE needs a nonempty set of real UIDs.
	ErrEmptyUIDSet = errors.New("UID MOVE requires a nonempty UID set")
	// ErrUIDNotFound reports that at least one requested UID is absent.
	ErrUIDNotFound = errors.New("some requested UIDs do not exist")
)

type UIDMapping struct {
	OldUID int64
	NewUID int64
}

type MoveResult struct {
	Mapping             []UIDMapping
	DestinationValidity int64
	SourceRevision      int64
	DestinationRevision int64
	SourceEvent         Event
	DestinationEvent    Event
}

type mailboxRow struct {
	id          int64
	name        string
	uidvalidity int64
	uidnext     int64
	revision    int64
}

// MoveMessages moves exactly the requested messages from source to destination
// as a single atomic operation:
//
//   - source and destination must already exist and differ by case-insensitive
//     identity;
//   - the UID set must be nonempty, contain no duplicates or wildcards, and
//     every requested UID must exist in the source (regardless of its flags);
//   - bodies and flags are copied byte-for-byte to fresh consecutive
//     destination UIDs in ascending source UID order, then the source rows are
//     removed. Unselected messages, including other \Deleted ones, are
//     untouched;
//   - one transaction covers every row plus both mailboxes' uidnext/revision
//     updates, so any failure (capacity, storage) leaves everything as it was;
//   - each mailbox advances exactly one revision and publishes exactly one
//     batched event.
func (s *Server) MoveMessages(actor *Session, source, destination string, uids []int64) (MoveResult, error) {
	result := MoveResult{}
	if len(uids) == 0 {
		return result, ErrEmptyUIDSet
	}
	srcCanonical := canonicalMailbox(source)
	dstCanonical := canonicalMailbox(destination)
	if srcCanonical == dstCanonical {
		return result, fmt.Errorf("source and destination must be different mailboxes")
	}

	srcHub := s.HubFor(srcCanonical)
	dstHub := s.HubFor(dstCanonical)
	// Deterministic lock order across the two mailboxes prevents an AB/BA
	// deadlock between two overlapping moves (A->B and B->A).
	first, second := srcHub, dstHub
	if srcCanonical > dstCanonical {
		first, second = dstHub, srcHub
	}
	first.mu.Lock()
	defer first.mu.Unlock()
	second.mu.Lock()
	defer second.mu.Unlock()

	err := withTx(s.db, func(tx *sql.Tx) error {
		src, err := lockMailboxRow(tx, srcCanonical)
		if err != nil {
			return err
		}
		dst, err := lockMailboxRow(tx, dstCanonical)
		if err != nil {
			return err
		}

		// Scan every source message in ascending UID order: sequence numbers
		// are positions within the full pre-batch listing (unselected messages
		// occupy positions too), and the scan both verifies existence and
		// preserves move order. Mailboxes cap at maxMessages rows.
		type pickedMessage struct {
			uid   int64
			flags string
			body  []byte
			seq   int
		}
		wanted := make(map[int64]bool, len(uids))
		for _, uid := range uids {
			wanted[uid] = true
		}
		rows, err := tx.Query(
			`SELECT uid, flags, body FROM messages WHERE mailbox_id = ? ORDER BY uid ASC`,
			src.id,
		)
		if err != nil {
			return err
		}
		var picked []pickedMessage
		seq := 0
		for rows.Next() {
			seq++
			var uid int64
			var encodedFlags string
			var body []byte
			if err := rows.Scan(&uid, &encodedFlags, &body); err != nil {
				rows.Close()
				return err
			}
			if wanted[uid] {
				picked = append(picked, pickedMessage{uid: uid, flags: encodedFlags, body: body, seq: seq})
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		if len(picked) != len(uids) {
			return ErrUIDNotFound
		}

		// Capacity is validated before any write so an overflow rolls back
		// without leaving copies behind.
		var srcCount, dstCount int
		if err := tx.QueryRow(
			`SELECT COUNT(*) FROM messages WHERE mailbox_id = ?`, src.id,
		).Scan(&srcCount); err != nil {
			return err
		}
		if err := tx.QueryRow(
			`SELECT COUNT(*) FROM messages WHERE mailbox_id = ?`, dst.id,
		).Scan(&dstCount); err != nil {
			return err
		}
		if dstCount+len(picked) > maxMessages {
			return ErrMailboxFull
		}

		// Fresh consecutive destination UIDs in ascending source UID order.
		mappings := make([]UIDMapping, 0, len(picked))
		newUIDs := make([]int64, 0, len(picked))
		oldUIDs := make([]int64, 0, len(picked))
		seqNums := make([]int, 0, len(picked))
		nextUID := dst.uidnext
		for _, item := range picked {
			if _, err := tx.Exec(
				`INSERT INTO messages(mailbox_id, uid, flags, body) VALUES(?, ?, ?, ?)`,
				dst.id, nextUID, item.flags, item.body,
			); err != nil {
				return err
			}
			mappings = append(mappings, UIDMapping{OldUID: item.uid, NewUID: nextUID})
			newUIDs = append(newUIDs, nextUID)
			oldUIDs = append(oldUIDs, item.uid)
			seqNums = append(seqNums, item.seq)
			nextUID++
		}
		for _, uid := range oldUIDs {
			if _, err := tx.Exec(
				`DELETE FROM messages WHERE mailbox_id = ? AND uid = ?`, src.id, uid,
			); err != nil {
				return err
			}
		}

		srcRevision := src.revision + 1
		dstRevision := dst.revision + 1
		if _, err := tx.Exec(
			`UPDATE mailboxes SET revision = ?, uidnext = ? WHERE id = ?`,
			dstRevision, nextUID, dst.id,
		); err != nil {
			return err
		}
		if _, err := tx.Exec(
			`UPDATE mailboxes SET revision = ? WHERE id = ?`, srcRevision, src.id,
		); err != nil {
			return err
		}

		result.Mapping = mappings
		result.DestinationValidity = dst.uidvalidity
		result.SourceRevision = srcRevision
		result.DestinationRevision = dstRevision
		result.DestinationEvent = Event{
			Type:        EventMoveIn,
			Mailbox:     dst.name,
			Revision:    dstRevision,
			Exists:      dstCount + len(picked),
			UIDValidity: dst.uidvalidity,
			UIDs:        newUIDs,
		}
		result.SourceEvent = Event{
			Type:     EventMoveOut,
			Mailbox:  src.name,
			Revision: srcRevision,
			Exists:   srcCount - len(picked),
			UIDs:     oldUIDs,
			SeqNums:  seqNums,
		}
		return nil
	})
	if err != nil {
		return result, err
	}

	// The transaction committed under both hub locks; publish both batches
	// before releasing either one so observers never see an intermediate state
	// and always observe a consistent revision order across the two mailboxes.
	dstHub.publishLocked(result.DestinationEvent, actor)
	srcHub.publishLocked(result.SourceEvent, actor)
	return result, nil
}

// lockMailboxRow loads a mailbox by case-insensitive name. It does not create
// missing mailboxes: MOVE targets must already exist.
func lockMailboxRow(tx *sql.Tx, canonical string) (mailboxRow, error) {
	var row mailboxRow
	err := tx.QueryRow(
		`SELECT id, name, uidvalidity, uidnext, revision FROM mailboxes
		 WHERE lower(name) = lower(?)`,
		canonical,
	).Scan(&row.id, &row.name, &row.uidvalidity, &row.uidnext, &row.revision)
	if errors.Is(err, sql.ErrNoRows) {
		return row, ErrNoSuchMailbox
	}
	return row, err
}
