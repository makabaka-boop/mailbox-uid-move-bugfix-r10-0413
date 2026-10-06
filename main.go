package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	_ "modernc.org/sqlite"
)

var (
	ErrNoSuchMailbox = errors.New("no such mailbox")
	ErrMailboxFull   = errors.New("mailbox full")
)

const (
	maxMessageBytes = 16 * 1024
	maxMessages     = 200
	sqliteBusyMS    = 5000
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9143", "TCP listen address")
	dbPath := flag.String("db", "mailbox.db", "SQLite database path")
	flag.Parse()

	db, err := openDatabase(*dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	server := NewServer(db)
	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("mailbox server listening on %s using %s", listener.Addr(), *dbPath)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
		_ = listener.Close()
	}()

	if err := server.Serve(listener); err != nil && !errors.Is(err, net.ErrClosed) {
		log.Fatal(err)
	}
	server.Wait()
}

func openDatabase(path string) (*sql.DB, error) {
	dsn := fmt.Sprintf(
		"file:%s?_pragma=busy_timeout(%d)&_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)",
		path, sqliteBusyMS,
	)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if err := initSchema(db); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func initSchema(db *sql.DB) error {
	_, err := db.Exec(`
CREATE TABLE IF NOT EXISTS mailboxes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    uidvalidity INTEGER NOT NULL,
    uidnext INTEGER NOT NULL DEFAULT 1,
    revision INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS messages (
    mailbox_id INTEGER NOT NULL REFERENCES mailboxes(id) ON DELETE CASCADE,
    uid INTEGER NOT NULL,
    flags TEXT NOT NULL DEFAULT '',
    body BLOB NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    PRIMARY KEY (mailbox_id, uid)
);
CREATE INDEX IF NOT EXISTS messages_mailbox_uid ON messages(mailbox_id, uid);
`)
	return err
}

type Server struct {
	db      *sql.DB
	hubs    map[string]*MailboxHub
	hubsMu  sync.Mutex
	conns   map[*Session]struct{}
	connsMu sync.Mutex
	wg      sync.WaitGroup
	closed  bool
}

func NewServer(db *sql.DB) *Server {
	return &Server{db: db, hubs: make(map[string]*MailboxHub), conns: make(map[*Session]struct{})}
}

func (s *Server) HubFor(name string) *MailboxHub {
	canonical := canonicalMailbox(name)
	s.hubsMu.Lock()
	defer s.hubsMu.Unlock()
	h := s.hubs[canonical]
	if h == nil {
		h = &MailboxHub{name: canonical, subscribers: make(map[*Session]struct{})}
		s.hubs[canonical] = h
	}
	return h
}

func (s *Server) Serve(listener net.Listener) error {
	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}
		session := NewSession(s, conn)
		s.connsMu.Lock()
		if s.closed {
			s.connsMu.Unlock()
			conn.Close()
			continue
		}
		s.conns[session] = struct{}{}
		s.connsMu.Unlock()
		s.wg.Add(1)
		go func() {
			defer func() {
				s.connsMu.Lock()
				delete(s.conns, session)
				s.connsMu.Unlock()
				s.wg.Done()
			}()
			session.Serve()
		}()
	}
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.connsMu.Lock()
	s.closed = true
	sessions := make([]*Session, 0, len(s.conns))
	for sess := range s.conns {
		sessions = append(sessions, sess)
	}
	s.connsMu.Unlock()

	for _, sess := range sessions {
		sess.Close()
	}
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Server) Wait() { s.wg.Wait() }

type MailboxInfo struct {
	ID          int64
	Name        string
	UIDValidity int64
	UIDNext     int64
	Revision    int64
	Exists      int
}

func (s *Server) GetMailbox(name string) (MailboxInfo, error) {
	var info MailboxInfo
	canonical := canonicalMailbox(name)
	err := s.db.QueryRow(`
SELECT m.id, m.name, m.uidvalidity, m.uidnext, m.revision, COUNT(q.uid)
FROM mailboxes m LEFT JOIN messages q ON q.mailbox_id = m.id
WHERE lower(m.name) = lower(?)
GROUP BY m.id`, canonical).Scan(
		&info.ID, &info.Name, &info.UIDValidity, &info.UIDNext, &info.Revision, &info.Exists,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return MailboxInfo{Name: canonical}, ErrNoSuchMailbox
	}
	return info, err
}

type AppendResult struct {
	UID      int64
	Revision int64
}

func (s *Server) AppendMessage(actor *Session, name string, body []byte, flags []string) (AppendResult, MailboxInfo, Event, error) {
	var result AppendResult
	var info MailboxInfo
	var ev Event
	hub := s.HubFor(name)
	canonical := hub.name
	hub.mu.Lock()
	defer hub.mu.Unlock()

	err := withTx(s.db, func(tx *sql.Tx) error {
		if _, err := tx.Exec(
			`INSERT INTO mailboxes(name, uidvalidity) VALUES(?, ?)
			 ON CONFLICT(name) DO NOTHING`,
			canonical, 1,
		); err != nil {
			return err
		}

		if err := tx.QueryRow(
			`SELECT id, name, uidvalidity, uidnext, revision FROM mailboxes WHERE lower(name) = lower(?)`,
			canonical,
		).Scan(&info.ID, &info.Name, &info.UIDValidity, &info.UIDNext, &info.Revision); err != nil {
			return err
		}

		var count int
		if err := tx.QueryRow(`SELECT COUNT(*) FROM messages WHERE mailbox_id = ?`, info.ID).Scan(&count); err != nil {
			return err
		}
		if count >= maxMessages {
			return ErrMailboxFull
		}

		result.UID = info.UIDNext
		if _, err := tx.Exec(
			`INSERT INTO messages(mailbox_id, uid, flags, body) VALUES(?, ?, ?, ?)`,
			info.ID, result.UID, encodeFlags(flags), body,
		); err != nil {
			return err
		}
		result.Revision = info.Revision + 1
		if _, err := tx.Exec(
			`UPDATE mailboxes SET uidnext = uidnext + 1, revision = ? WHERE id = ?`,
			result.Revision, info.ID,
		); err != nil {
			return err
		}
		info.UIDNext = result.UID + 1
		info.Revision = result.Revision
		info.Exists = count + 1
		ev = Event{
			Type:        EventAppend,
			Mailbox:     info.Name,
			Revision:    result.Revision,
			Exists:      info.Exists,
			UIDValidity: info.UIDValidity,
			UID:         result.UID,
		}
		return nil
	})
	if err == nil {
		hub.publishLocked(ev, actor)
	}
	return result, info, ev, err
}

type MessageInfo struct {
	Seq   int
	UID   int64
	Flags []string
	Body  []byte
}

func (s *Server) FetchMessages(name string, uids []int64, wantAll bool) ([]MessageInfo, error) {
	info, err := s.GetMailbox(name)
	if err != nil {
		return nil, err
	}
	query := `SELECT uid, flags, body FROM messages WHERE mailbox_id = ? ORDER BY uid ASC`
	rows, err := s.db.Query(query, info.ID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	wanted := make(map[int64]bool, len(uids))
	for _, uid := range uids {
		wanted[uid] = true
	}
	var messages []MessageInfo
	seq := 0
	for rows.Next() {
		var msg MessageInfo
		var flags string
		if err := rows.Scan(&msg.UID, &flags, &msg.Body); err != nil {
			return nil, err
		}
		seq++
		if len(wanted) > 0 && !wanted[msg.UID] {
			continue
		}
		msg.Seq = seq
		msg.Flags = decodeFlags(flags)
		if !wantAll {
			msg.Body = nil
		}
		messages = append(messages, msg)
	}
	return messages, rows.Err()
}

type StoreResult struct {
	Revision int64
	Messages []MessageInfo
}

func (s *Server) StoreFlags(actor *Session, name string, uids []int64, action flagAction, flags []string, silent bool) (StoreResult, MailboxInfo, error) {
	var store StoreResult
	info, err := s.GetMailbox(name)
	if err != nil {
		return store, info, err
	}

	// Lock the mailbox hub so every committed change gets one total revision order.
	hub := s.HubFor(name)
	hub.mu.Lock()
	defer hub.mu.Unlock()
	info, err = s.GetMailbox(name)
	if err != nil {
		return store, info, err
	}

	err = withTx(s.db, func(tx *sql.Tx) error {
		rows, err := tx.Query(
			`SELECT uid, flags FROM messages WHERE mailbox_id = ? ORDER BY uid ASC`, info.ID,
		)
		if err != nil {
			return err
		}
		type selected struct {
			seq   int
			uid   int64
			flags map[string]string
		}
		wanted := make(map[int64]bool, len(uids))
		for _, uid := range uids {
			wanted[uid] = true
		}
		var picked []selected
		seq := 0
		for rows.Next() {
			seq++
			var uid int64
			var oldFlags string
			if err := rows.Scan(&uid, &oldFlags); err != nil {
				rows.Close()
				return err
			}
			if len(wanted) > 0 && !wanted[uid] {
				continue
			}
			picked = append(picked, selected{seq: seq, uid: uid, flags: flagMap(decodeFlags(oldFlags))})
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}

		changes := 0
		for _, item := range picked {
			before := encodeMapFlags(item.flags)
			switch action {
			case flagSet:
				item.flags = flagMap(flags)
			case flagAdd:
				for _, flag := range flags {
					item.flags[canonicalFlag(flag)] = flag
				}
			case flagRemove:
				for _, flag := range flags {
					delete(item.flags, canonicalFlag(flag))
				}
			}
			after := encodeMapFlags(item.flags)
			if before != after {
				changes++
				if _, err := tx.Exec(
					`UPDATE messages SET flags = ? WHERE mailbox_id = ? AND uid = ?`,
					after, info.ID, item.uid,
				); err != nil {
					return err
				}
			}
			store.Messages = append(store.Messages, MessageInfo{Seq: item.seq, UID: item.uid, Flags: decodeFlags(after)})
		}

		if changes > 0 {
			store.Revision = info.Revision + 1
			if _, err := tx.Exec(`UPDATE mailboxes SET revision = ? WHERE id = ?`, store.Revision, info.ID); err != nil {
				return err
			}
		} else {
			store.Revision = info.Revision
		}
		return nil
	})
	if err == nil {
		info.Revision = store.Revision
		var countErr error
		info.Exists, countErr = countMessages(s.db, info.ID)
		if countErr != nil {
			err = countErr
		}
		if err == nil && store.Revision != 0 {
			hub.publishLocked(Event{
				Type:     EventStore,
				Mailbox:  info.Name,
				Revision: store.Revision,
				Messages: store.Messages,
			}, actor)
		}
	}
	return store, info, err
}

type ExpungeResult struct {
	Revision int64
	UIDs     []int64
	SeqNums  []int
}

func (s *Server) ExpungeDeleted(actor *Session, name string) (ExpungeResult, MailboxInfo, error) {
	var result ExpungeResult
	info, err := s.GetMailbox(name)
	if err != nil {
		return result, info, err
	}
	hub := s.HubFor(name)
	hub.mu.Lock()
	defer hub.mu.Unlock()
	info, err = s.GetMailbox(name)
	if err != nil {
		return result, info, err
	}

	err = withTx(s.db, func(tx *sql.Tx) error {
		rows, err := tx.Query(
			`SELECT uid, flags FROM messages WHERE mailbox_id = ? ORDER BY uid ASC`, info.ID,
		)
		if err != nil {
			return err
		}
		type deleted struct {
			uid int64
			seq int
		}
		var toDelete []deleted
		seq := 0
		for rows.Next() {
			seq++
			var uid int64
			var encoded string
			if err := rows.Scan(&uid, &encoded); err != nil {
				rows.Close()
				return err
			}
			flags := flagMap(decodeFlags(encoded))
			if _, ok := flags[canonicalFlag(`\Deleted`)]; ok {
				toDelete = append(toDelete, deleted{uid: uid, seq: seq})
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		if len(toDelete) == 0 {
			result.Revision = info.Revision
			return nil
		}

		for _, item := range toDelete {
			if _, err := tx.Exec(`DELETE FROM messages WHERE mailbox_id = ? AND uid = ?`, info.ID, item.uid); err != nil {
				return err
			}
			result.UIDs = append(result.UIDs, item.uid)
			result.SeqNums = append(result.SeqNums, item.seq)
		}
		result.Revision = info.Revision + 1
		if _, err := tx.Exec(`UPDATE mailboxes SET revision = ? WHERE id = ?`, result.Revision, info.ID); err != nil {
			return err
		}
		return nil
	})
	if err == nil {
		info.Revision = result.Revision
		var countErr error
		info.Exists, countErr = countMessages(s.db, info.ID)
		if countErr != nil {
			err = countErr
		}
		if err == nil && len(result.UIDs) > 0 {
			hub.publishLocked(Event{
				Type:     EventExpunge,
				Mailbox:  info.Name,
				Revision: result.Revision,
				UIDs:     result.UIDs,
				SeqNums:  result.SeqNums,
				Exists:   info.Exists,
			}, actor)
		}
	}
	return result, info, err
}

func countMessages(db *sql.DB, mailboxID int64) (int, error) {
	var count int
	err := db.QueryRow(`SELECT COUNT(*) FROM messages WHERE mailbox_id = ?`, mailboxID).Scan(&count)
	return count, err
}

func withTx(db *sql.DB, fn func(*sql.Tx) error) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}
