package main

import (
	"database/sql"
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// restartableServer is a test server whose database file can be reopened with a
// fresh Server process, to verify UID allocation survives a restart.
type restartableServer struct {
	path string
	db   *sql.DB
	ln   net.Listener
	srv  *Server
}

func startRestartable(t *testing.T) (string, *restartableServer) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "mailbox.db")
	db, err := openDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	srv := NewServer(db)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve(ln)
	return ln.Addr().String(), &restartableServer{path: path, db: db, ln: ln, srv: srv}
}

func (r *restartableServer) restart(t *testing.T) string {
	t.Helper()
	_ = r.ln.Close()
	r.srv.Wait()
	if err := r.db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := openDatabase(r.path)
	if err != nil {
		t.Fatal(err)
	}
	srv := NewServer(db)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve(ln)
	r.db = db
	r.ln = ln
	r.srv = srv
	t.Cleanup(func() {
		ln.Close()
		srv.Wait()
		db.Close()
	})
	return ln.Addr().String()
}

// fetchAll issues UID FETCH with UID FLAGS BODY[] and returns the FETCH header
// lines (in response order) and a UID->body map.
func (c *testClient) fetchAll(tag, uidSet string) ([]string, map[int64]string) {
	c.t.Helper()
	c.send("%s UID FETCH %s (UID FLAGS BODY[])\r\n", tag, uidSet)
	headers := []string{}
	bodies := map[int64]string{}
	for {
		line := c.line()
		if strings.HasPrefix(line, tag+" ") {
			return headers, bodies
		}
		if !strings.HasPrefix(line, "* ") || !strings.Contains(line, " FETCH (") {
			c.t.Fatalf("unexpected fetch line %q", line)
		}
		idx := strings.Index(line, "{")
		if idx < 0 {
			c.t.Fatalf("missing literal marker in %q", line)
		}
		size, err := strconv.Atoi(strings.TrimSuffix(line[idx+1:], "}"))
		if err != nil {
			c.t.Fatalf("bad literal size in %q: %v", line, err)
		}
		rest := strings.SplitN(line, "UID ", 2)[1]
		uid, err := strconv.ParseInt(strings.SplitN(rest, " ", 2)[0], 10, 64)
		if err != nil {
			c.t.Fatalf("bad UID in %q", line)
		}
		headers = append(headers, line)
		bodies[uid] = readBody(c, size)
		c.expectLine(")")
	}
}

func expectQuiet(t *testing.T, c *testClient, reason string) {
	t.Helper()
	c.conn.SetReadDeadline(time.Now().Add(250 * time.Millisecond))
	if got, err := c.br.ReadString('\n'); err == nil {
		t.Fatalf("%s: unexpected notification %q", reason, got)
	}
	c.conn.SetReadDeadline(time.Time{})
}

// TestUIDMoveAtomicBatch drives one real move over TCP with observers on both
// mailboxes. It checks that exactly the selected UIDs move, unselected
// \Deleted mail survives, bodies and flags are preserved, destination gets
// fresh consecutive UIDs, each mailbox advances exactly one revision, and both
// observers see a single committed batch with no intermediate states.
func TestUIDMoveAtomicBatch(t *testing.T) {
	srv := startTestServer(t)
	a := dialClient(t, srv.addr) // actor, selected INBOX
	b := dialClient(t, srv.addr) // observer, selected INBOX
	c := dialClient(t, srv.addr) // observer, selected ARCHIVE

	a.appendMsg("P1", "INBOX", []byte("body-one"))
	a.expectLine("* 1 EXISTS")
	a.expectContains("P1 OK")
	a.appendMsg("P2", "INBOX", []byte("body-two\r\nCRLF and {5} literal text"), `\Seen`)
	a.expectLine("* 2 EXISTS")
	a.expectContains("P2 OK")
	a.appendMsg("P3", "INBOX", []byte("body-three"))
	a.expectLine("* 3 EXISTS")
	a.expectContains("P3 OK")
	a.appendMsg("P4", "INBOX", []byte("body-four"), `\Deleted`, `\Flagged`)
	a.expectLine("* 4 EXISTS")
	a.expectContains("P4 OK")
	a.appendMsg("P5", "INBOX", []byte("body-five"))
	a.expectLine("* 5 EXISTS")
	a.expectContains("P5 OK")

	// Destination must exist beforehand.
	a.appendMsg("Q1", "ARCHIVE", []byte("archive-keeper"))
	a.expectLine("* 1 EXISTS")
	a.expectContains("Q1 OK")

	a.send("A0 SELECT INBOX\r\n")
	a.expectLine("* 5 EXISTS")
	a.expectContains("[UIDVALIDITY")
	a.expectContains("[UIDNEXT 6]")
	a.expectContains("[REVISION 5]")
	a.expectStatus("A0", "OK")

	b.send("B0 SELECT INBOX\r\n")
	b.expectLine("* 5 EXISTS")
	b.expectContains("[UIDVALIDITY")
	b.expectContains("[UIDNEXT 6]")
	b.expectContains("[REVISION 5]")
	b.expectStatus("B0", "OK")

	c.send("C0 SELECT ARCHIVE\r\n")
	c.expectLine("* 1 EXISTS")
	c.expectContains("[UIDVALIDITY")
	c.expectContains("[UIDNEXT 2]")
	c.expectContains("[REVISION 1]")
	c.expectStatus("C0", "OK")

	// Move UID 2 (\Seen) and UID 4 (\Deleted \Flagged); UID 3 is left behind
	// even though UID 4 carries \Deleted.
	a.send("A1 UID MOVE 2,4 ArChIvE\r\n")
	a.expectLine("* 2 EXPUNGE")
	a.expectLine("* 4 EXPUNGE")
	a.expectLine("* 3 EXISTS")
	a.expectContains("A1 OK [COPYUID 1 2,4 2,3] [REVISION 6]")

	// Destination observer: one append batch, one revision, no per-message
	// intermediate revisions. Archive held one keeper, so it now has three.
	c.expectLine("* 3 EXISTS")
	c.expectLine("* APPENDUID 1 2")
	c.expectLine("* APPENDUID 1 3")
	c.expectLine("* REVISION 2")

	// Source observer: one expunge batch with pre-batch sequence numbers.
	b.expectLine("* 2 EXPUNGE")
	b.expectLine("* 4 EXPUNGE")
	b.expectLine("* 3 EXISTS")
	b.expectLine("* REVISION 6")

	// Source kept exactly UIDs 1, 3 and 5 with renumbered sequence positions.
	headers, bodies := a.fetchAll("A2", "1:5")
	gotUIDs := headerUIDs(t, headers)
	if strings.Join(gotUIDs, ",") != "1,3,5" {
		t.Fatalf("source UIDs after move = %v, want 1,3,5", gotUIDs)
	}
	seqs := headerSeqs(t, headers)
	if strings.Join(seqs, ",") != "1,2,3" {
		t.Fatalf("source sequence numbers = %v, want 1,2,3", seqs)
	}
	if bodies[1] != "body-one" || bodies[3] != "body-three" || bodies[5] != "body-five" {
		t.Fatalf("source bodies = %#v", bodies)
	}

	// Destination now holds the keeper plus the two moved messages, bodies and
	// flags copied byte-for-byte; UIDs 2 and 3 are consecutive.
	cheaders, cbodies := c.fetchAll("C1", "1:3")
	if got := strings.Join(headerUIDs(t, cheaders), ","); got != "1,2,3" {
		t.Fatalf("destination UIDs = %v, want 1,2,3", got)
	}
	if cbodies[1] != "archive-keeper" {
		t.Fatalf("keeper body = %q", cbodies[1])
	}
	if cbodies[2] != "body-two\r\nCRLF and {5} literal text" {
		t.Fatalf("moved body-two = %q", cbodies[2])
	}
	if cbodies[3] != "body-four" {
		t.Fatalf("moved body-four = %q", cbodies[3])
	}
	if !strings.Contains(cheaders[1], "FLAGS (\\Seen)") {
		t.Fatalf("moved uid2 flags lost: %q", cheaders[1])
	}
	if !strings.Contains(cheaders[2], "FLAGS (\\Deleted \\Flagged)") {
		t.Fatalf("moved uid4 flags lost: %q", cheaders[2])
	}

	// Allocation after the move keeps climbing on both mailboxes.
	a.appendMsg("Q2", "ARCHIVE", []byte("archive-new"))
	a.expectLine("* 4 EXISTS")
	a.expectContains("Q2 OK [APPENDUID 1 4] [REVISION 3]")
	c.expectLine("* 4 EXISTS")
	c.expectLine("* APPENDUID 1 4")
	c.expectLine("* REVISION 3")

	a.appendMsg("P6", "INBOX", []byte("body-six"))
	a.expectLine("* 4 EXISTS")
	a.expectContains("P6 OK [APPENDUID 1 6] [REVISION 7]")
	b.expectLine("* 4 EXISTS")
	b.expectLine("* APPENDUID 1 6")
	b.expectLine("* REVISION 7")

	// Cross-mailbox isolation: each observer only saw its own mailbox.
	expectQuiet(t, c, "archive observer after INBOX append")
	expectQuiet(t, b, "inbox observer after ARCHIVE append")
}

// TestUIDMoveFailuresAndInvalidSelection covers missing mailboxes, same
// mailbox (case-insensitive), missing/duplicate/wildcard UIDs, and the
// destination capacity limit. Every failure leaves messages, UIDNEXT,
// revisions and notification streams on both sides untouched.
func TestUIDMoveFailuresAndInvalidSelection(t *testing.T) {
	srv := startTestServer(t)
	a := dialClient(t, srv.addr)
	b := dialClient(t, srv.addr)
	c := dialClient(t, srv.addr)

	for i, body := range []string{"one", "two", "three"} {
		a.appendMsg("P"+strconv.Itoa(i+1), "INBOX", []byte(body))
		a.expectLine(fmt.Sprintf("* %d EXISTS", i+1))
		a.expectContains("P" + strconv.Itoa(i+1) + " OK")
	}
	a.appendMsg("Q1", "ARCHIVE", []byte("keeper"))
	a.expectLine("* 1 EXISTS")
	a.expectContains("Q1 OK")

	a.send("A0 SELECT INBOX\r\n")
	a.expectLine("* 3 EXISTS")
	a.expectContains("[UIDVALIDITY")
	a.expectContains("[UIDNEXT 4]")
	a.expectContains("[REVISION 3]")
	a.expectStatus("A0", "OK")
	b.send("B0 SELECT INBOX\r\n")
	b.expectLine("* 3 EXISTS")
	b.expectContains("[UIDVALIDITY")
	b.expectContains("[UIDNEXT 4]")
	b.expectContains("[REVISION 3]")
	b.expectStatus("B0", "OK")
	c.send("C0 SELECT ARCHIVE\r\n")
	c.expectLine("* 1 EXISTS")
	c.expectContains("[UIDVALIDITY")
	c.expectContains("[UIDNEXT 2]")
	c.expectContains("[REVISION 1]")
	c.expectStatus("C0", "OK")

	failures := []struct{ tag, cmd string }{
		{"M1", "M1 UID MOVE 1 NOWHERE\r\n"},
		{"M2", "M2 UID MOVE 1 INBOX\r\n"},
		{"M3", "M3 UID MOVE 1 inbox\r\n"},       // same mailbox, different case
		{"M4", "M4 UID MOVE 9 ARCHIVE\r\n"},     // missing UID
		{"M5", "M5 UID MOVE 1,9 ARCHIVE\r\n"},   // one missing UID rejects the batch
		{"M6", "M6 UID MOVE 1,1 ARCHIVE\r\n"},   // duplicate
		{"M7", "M7 UID MOVE 1:2,2 ARCHIVE\r\n"}, // duplicate across range
		{"M8", "M8 UID MOVE 1:* ARCHIVE\r\n"},   // wildcard
		{"M9", "M9 UID MOVE * ARCHIVE\r\n"},     // empty/wildcard
	}
	for _, f := range failures {
		a.send("%s", f.cmd)
		line := a.expectContains(f.tag + " NO")
		if strings.Contains(line, "BAD") {
			t.Fatalf("%s rejected as protocol error: %q", f.tag, line)
		}
	}

	// Nothing moved: source still has all three, revision unchanged.
	headers, _ := a.fetchAll("F0", "1:3")
	if got := strings.Join(headerUIDs(t, headers), ","); got != "1,2,3" {
		t.Fatalf("source UIDs after failed moves = %v, want 1,2,3", got)
	}
	a.send("F1 SELECT INBOX\r\n")
	a.expectLine("* 3 EXISTS")
	a.expectContains("[UIDVALIDITY")
	a.expectContains("[UIDNEXT 4]")
	a.expectContains("[REVISION 3]")
	a.expectStatus("F1", "OK")

	// Destination untouched as well, no notifications anywhere.
	d := dialClient(t, srv.addr)
	d.send("D1 SELECT ARCHIVE\r\n")
	d.expectLine("* 1 EXISTS")
	d.expectContains("[UIDVALIDITY")
	d.expectContains("[UIDNEXT 2]")
	d.expectContains("[REVISION 1]")
	d.expectStatus("D1", "OK")
	expectQuiet(t, b, "source observer after failed moves")
	expectQuiet(t, c, "destination observer after failed moves")

	// Fill the destination to its 200-message cap (1 keeper + 199 appends).
	for i := 2; i <= maxMessages; i++ {
		a.appendMsg("F"+pad3(i), "ARCHIVE", []byte("x"))
		// a is selected on INBOX; this EXISTS describes the destination count,
		// so it is not part of INBOX's revision stream — discard it.
		a.expectContains("EXISTS")
		a.expectContains("F" + pad3(i) + " OK")
		c.drainUntilRevision(i) // archive revision advances one per append
		d.drainUntilRevision(i)
	}

	// A one-message move must fail atomically: no copies are left behind and
	// UIDNEXT/revision on both sides stay put.
	a.send("Z1 UID MOVE 1 ARCHIVE\r\n")
	a.expectContains("Z1 NO mailbox full")
	headers, _ = a.fetchAll("Z2", "1:3")
	if got := strings.Join(headerUIDs(t, headers), ","); got != "1,2,3" {
		t.Fatalf("source UIDs after capacity failure = %v", got)
	}
	d.send("D2 SELECT ARCHIVE\r\n")
	d.expectLine("* 200 EXISTS")
	d.expectContains("[UIDVALIDITY")
	d.expectContains("[UIDNEXT 201]")
	d.expectContains("[REVISION 200]")
	d.expectStatus("D2", "OK")

	// Free one slot: a two-message batch still fails (whole batch must fit),
	// while a one-message move then succeeds and consumes fresh UID 201.
	c.send("C2 UID STORE 200 FLAGS (\\Deleted)\r\n")
	c.expectLine("* 200 FETCH (UID 200 FLAGS (\\Deleted))")
	c.expectContains("[REVISION 201]")
	c.send("C3 EXPUNGE\r\n")
	c.expectLine("* 200 EXPUNGE")
	c.expectLine("* 199 EXISTS")
	c.expectContains("[REVISION 202]")

	a.send("Z3 UID MOVE 2,3 ARCHIVE\r\n")
	a.expectContains("Z3 NO mailbox full")
	a.send("Z4 UID MOVE 2 ARCHIVE\r\n")
	a.expectLine("* 2 EXPUNGE")
	a.expectLine("* 2 EXISTS")
	a.expectContains("Z4 OK [COPYUID 1 2 201] [REVISION 4]")
	c.expectLine("* 200 EXISTS")
	c.expectLine("* APPENDUID 1 201")
	c.expectLine("* REVISION 203")

	// UID 3, though never selected, still carries no deletions; source is 1,3.
	headers, _ = a.fetchAll("Z5", "1:3")
	if got := strings.Join(headerUIDs(t, headers), ","); got != "1,3" {
		t.Fatalf("source UIDs = %v, want 1,3", got)
	}
}

// TestUIDMoveCaseInsensitiveMailbox verifies mailbox identity is case
// insensitive for selection, lock identity and the source/destination
// comparison.
func TestUIDMoveCaseInsensitiveMailbox(t *testing.T) {
	srv := startTestServer(t)
	a := dialClient(t, srv.addr)
	b := dialClient(t, srv.addr)
	c := dialClient(t, srv.addr)

	a.appendMsg("P1", "INBOX", []byte("alpha"))
	a.expectLine("* 1 EXISTS")
	a.expectContains("P1 OK")
	a.appendMsg("P2", "INBOX", []byte("beta"))
	a.expectLine("* 2 EXISTS")
	a.expectContains("P2 OK")
	a.appendMsg("Q1", "Archive", []byte("keeper"))
	a.expectLine("* 1 EXISTS")
	a.expectContains("Q1 OK")

	a.send("A0 SELECT INBOX\r\n")
	a.expectLine("* 2 EXISTS")
	a.expectContains("[UIDVALIDITY")
	a.expectContains("[UIDNEXT")
	a.expectContains("[REVISION 2]")
	a.expectStatus("A0", "OK")
	// Selecting "inbox" must subscribe to the same mailbox hub.
	b.send("B0 SELECT inbox\r\n")
	b.expectLine("* 2 EXISTS")
	b.expectContains("[UIDVALIDITY")
	b.expectContains("[UIDNEXT")
	b.expectContains("[REVISION 2]")
	b.expectStatus("B0", "OK")
	// And "ARCHIVE" is the same mailbox as "Archive".
	c.send("C0 SELECT ARCHIVE\r\n")
	c.expectLine("* 1 EXISTS")
	c.expectContains("[UIDVALIDITY")
	c.expectContains("[UIDNEXT")
	c.expectContains("[REVISION 1]")
	c.expectStatus("C0", "OK")

	a.send("A1 UID MOVE 1 aRcHiVe\r\n")
	a.expectLine("* 1 EXPUNGE")
	a.expectLine("* 1 EXISTS")
	a.expectContains("A1 OK [COPYUID 1 1 2] [REVISION 3]")
	b.expectLine("* 1 EXPUNGE")
	b.expectLine("* 1 EXISTS")
	b.expectLine("* REVISION 3")
	c.expectLine("* 2 EXISTS")
	c.expectLine("* APPENDUID 1 2")
	c.expectLine("* REVISION 2")

	headers, bodies := c.fetchAll("C1", "1:2")
	if got := strings.Join(headerUIDs(t, headers), ","); got != "1,2" {
		t.Fatalf("destination UIDs = %v", got)
	}
	if bodies[2] != "alpha" {
		t.Fatalf("moved body = %q", bodies[2])
	}
}

// TestUIDMoveRestartNoUIDReuse checks that after reopening the database the
// moved messages, mapping results and both mailboxes' UIDNEXT are persisted,
// and subsequent appends never reuse old UIDs.
func TestUIDMoveRestartNoUIDReuse(t *testing.T) {
	addr, rs := startRestartable(t)
	a := dialClient(t, addr)

	a.appendMsg("P1", "INBOX", []byte("move-me"))
	a.expectLine("* 1 EXISTS")
	a.expectContains("P1 OK")
	a.appendMsg("P2", "INBOX", []byte("stay"), `\Seen`)
	a.expectLine("* 2 EXISTS")
	a.expectContains("P2 OK")
	a.appendMsg("Q1", "ARCHIVE", []byte("keeper"))
	a.expectLine("* 1 EXISTS")
	a.expectContains("Q1 OK")

	a.send("A0 SELECT INBOX\r\n")
	a.expectLine("* 2 EXISTS")
	a.expectContains("[UIDVALIDITY")
	a.expectContains("[UIDNEXT")
	a.expectContains("[REVISION 2]")
	a.expectStatus("A0", "OK")
	a.send("A1 UID MOVE 1 ARCHIVE\r\n")
	a.expectLine("* 1 EXPUNGE")
	a.expectLine("* 1 EXISTS")
	a.expectContains("A1 OK [COPYUID 1 1 2] [REVISION 3]")

	a.conn.Close()
	addr = rs.restart(t)
	d := dialClient(t, addr)

	// Source kept UID 2; destination kept UIDs 1 and 2.
	d.send("D0 SELECT INBOX\r\n")
	d.expectLine("* 1 EXISTS")
	d.expectContains("[UIDVALIDITY")
	d.expectContains("[UIDNEXT 3]")
	d.expectContains("[REVISION 3]")
	d.expectStatus("D0", "OK")
	d.send("D1 SELECT ARCHIVE\r\n")
	d.expectLine("* 2 EXISTS")
	d.expectContains("[UIDVALIDITY")
	d.expectContains("[UIDNEXT 3]")
	d.expectContains("[REVISION 2]")
	d.expectStatus("D1", "OK")

	headers, bodies := d.fetchAll("D2", "1:2")
	if got := strings.Join(headerUIDs(t, headers), ","); got != "1,2" {
		t.Fatalf("archive UIDs after restart = %v, want 1,2", got)
	}
	if bodies[1] != "keeper" || bodies[2] != "move-me" {
		t.Fatalf("bodies after restart = %#v", bodies)
	}

	// New allocations after restart continue past the pre-restart UIDNEXT.
	d.appendMsg("D3", "INBOX", []byte("after"))
	d.expectLine("* 2 EXISTS")
	d.expectContains("D3 OK [APPENDUID 1 3] [REVISION 4]")
	d.appendMsg("D4", "ARCHIVE", []byte("after"))
	d.expectLine("* 3 EXISTS")
	d.expectContains("D4 OK [APPENDUID 1 3] [REVISION 3]")
}

func headerUIDs(t *testing.T, headers []string) []string {
	t.Helper()
	out := make([]string, 0, len(headers))
	for _, h := range headers {
		rest := strings.SplitN(h, "UID ", 2)[1]
		out = append(out, strings.SplitN(rest, " ", 2)[0])
	}
	return out
}

func headerSeqs(t *testing.T, headers []string) []string {
	t.Helper()
	out := make([]string, 0, len(headers))
	for _, h := range headers {
		rest := strings.TrimPrefix(h, "* ")
		out = append(out, strings.SplitN(rest, " ", 2)[0])
	}
	return out
}

func pad3(n int) string {
	if n < 10 {
		return "00" + strconv.Itoa(n)
	}
	if n < 100 {
		return "0" + strconv.Itoa(n)
	}
	return strconv.Itoa(n)
}
