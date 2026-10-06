package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"net"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

type testServer struct {
	db   *sql.DB
	srv  *Server
	ln   net.Listener
	addr string
}

func startTestServer(t *testing.T) *testServer {
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
	t.Cleanup(func() {
		ln.Close()
		srv.Wait()
		db.Close()
	})
	return &testServer{srv: srv, ln: ln, addr: ln.Addr().String()}
}

type testClient struct {
	t    *testing.T
	conn net.Conn
	br   *bufio.Reader
}

func dialClient(t *testing.T, addr string) *testClient {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	c := &testClient{t: t, conn: conn, br: bufio.NewReader(conn)}
	t.Cleanup(func() { conn.Close() })
	c.expectLine("* OK custom mailbox service ready")
	return c
}

func (c *testClient) send(format string, args ...any) {
	c.t.Helper()
	if _, err := fmt.Fprintf(c.conn, format, args...); err != nil {
		c.t.Fatal(err)
	}
}

func (c *testClient) sendRaw(data []byte) {
	c.t.Helper()
	if _, err := c.conn.Write(data); err != nil {
		c.t.Fatal(err)
	}
}

func (c *testClient) closeWrite() {
	if tc, ok := c.conn.(*net.TCPConn); ok {
		if err := tc.CloseWrite(); err != nil {
			c.t.Fatal(err)
		}
	}
}

func (c *testClient) line() string {
	c.t.Helper()
	c.conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	line, err := c.br.ReadString('\n')
	if err != nil {
		c.t.Fatalf("read response line: %v", err)
	}
	return strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
}

func (c *testClient) expectLine(want string) string {
	c.t.Helper()
	got := c.line()
	if got != want {
		c.t.Fatalf("response = %q, want %q", got, want)
	}
	return got
}

func (c *testClient) expectContains(want string) string {
	c.t.Helper()
	got := c.line()
	if !strings.Contains(got, want) {
		c.t.Fatalf("response = %q, want substring %q", got, want)
	}
	return got
}

func (c *testClient) expectStatus(tag, status string) {
	c.t.Helper()
	got := c.line()
	wantPrefix := tag + " " + status + " "
	if !strings.HasPrefix(got, wantPrefix) {
		c.t.Fatalf("status = %q, want prefix %q", got, wantPrefix)
	}
}

func (c *testClient) appendMsg(tag, mailbox string, body []byte, flags ...string) {
	c.t.Helper()
	flagText := ""
	if len(flags) > 0 {
		flagText = " (" + strings.Join(flags, " ") + ")"
	}
	c.send("%s APPEND %s%s {%d}\r\n", tag, mailbox, flagText, len(body))
	c.sendRaw(body)
	c.sendRaw([]byte("\r\n"))
}

func (c *testClient) drainUntilRevision(want int) []string {
	c.t.Helper()
	var lines []string
	deadline := time.Now().Add(3 * time.Second)
	for {
		c.conn.SetReadDeadline(deadline)
		line := c.line()
		lines = append(lines, line)
		if strings.HasPrefix(line, "* REVISION ") || strings.Contains(line, "[REVISION ") {
			n := revisionInLine(line)
			if n == want {
				return lines
			}
			if n > want {
				c.t.Fatalf("saw revision %d before expected %d in %q", n, want, lines)
			}
		}
	}
}

func revisionInLine(line string) int {
	re := regexp.MustCompile(`(?:\* REVISION |\[REVISION )([0-9]+)`)
	m := re.FindStringSubmatch(line)
	if m == nil {
		return -1
	}
	n, err := strconv.Atoi(m[1])
	if err != nil {
		return -1
	}
	return n
}

func readBody(c *testClient, n int) string {
	c.t.Helper()
	c.conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	body := make([]byte, n)
	if _, err := readFull(c.br, body); err != nil {
		c.t.Fatal(err)
	}
	// literal terminator
	two := make([]byte, 2)
	if _, err := readFull(c.br, two); err != nil {
		c.t.Fatal(err)
	}
	if string(two) != "\r\n" {
		c.t.Fatalf("literal terminator = %q", two)
	}
	return string(body)
}

func readFull(r interface{ Read([]byte) (int, error) }, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := r.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func TestTwoClientsInterleavedMutationsAndRevisions(t *testing.T) {
	srv := startTestServer(t)
	a := dialClient(t, srv.addr)
	b := dialClient(t, srv.addr)

	// Create INBOX through A's first append. B selects afterward and gets rev 1.
	body1 := []byte("first message\r\nA1 APPEND INBOX {99}\r\n* EXPUNGE command-looking payload")
	a.appendMsg("A1", "INBOX", body1)
	a.expectLine("* 1 EXISTS")
	a.expectContains("A1 OK [APPENDUID 1 1] [REVISION 1]")

	a.send("A0 SELECT INBOX\r\n")
	a.expectLine("* 1 EXISTS")
	a.expectContains("[UIDVALIDITY 1]")
	a.expectContains("[UIDNEXT 2]")
	a.expectContains("[REVISION 1]")
	a.expectStatus("A0", "OK")

	b.send("B1 SELECT INBOX\r\n")
	b.expectLine("* 1 EXISTS")
	b.expectContains("[UIDVALIDITY")
	b.expectContains("[UIDNEXT 2]")
	b.expectContains("[REVISION 1]")
	b.expectStatus("B1", "OK")

	// Both selected; interleave append, flag change, and append.
	a.appendMsg("A2", "INBOX", []byte("second\r\nBODY[] {5}\r\nA1 BAD still payload"))
	a.expectLine("* 2 EXISTS")
	a.expectContains("[APPENDUID 1 2] [REVISION 2]")
	notices := b.drainUntilRevision(2)
	if !containsLine(notices, "* 2 EXISTS") || !containsLine(notices, "* APPENDUID 1 2") {
		t.Fatalf("B append notices = %#v", notices)
	}

	a.send("A3 UID STORE 1 +FLAGS (\\Seen)\r\n")
	a.expectLine("* 1 FETCH (UID 1 FLAGS (\\Seen))")
	a.expectContains("[REVISION 3]")
	notices = b.drainUntilRevision(3)
	if !containsLine(notices, "* 1 FETCH (UID 1 FLAGS (\\Seen))") {
		t.Fatalf("B store notices = %#v", notices)
	}

	b.appendMsg("B2", "INBOX", []byte("third"))
	b.expectLine("* 3 EXISTS")
	b.expectContains("[APPENDUID 1 3] [REVISION 4]")
	notices = a.drainUntilRevision(4)
	if !containsLine(notices, "* 3 EXISTS") || !containsLine(notices, "* APPENDUID 1 3") {
		t.Fatalf("A append notices = %#v", notices)
	}

	// Mark old UID 2 deleted, then UID 1. EXPUNGE sequence numbers are 2 and 3.
	a.send("A4 UID STORE 2 +FLAGS (\\Deleted)\r\n")
	a.expectLine("* 2 FETCH (UID 2 FLAGS (\\Deleted))")
	a.expectContains("[REVISION 5]")
	b.drainUntilRevision(5)

	b.send("B3 UID STORE 1 +FLAGS (\\Deleted)\r\n")
	b.expectLine("* 1 FETCH (UID 1 FLAGS (\\Deleted \\Seen))")
	b.expectContains("[REVISION 6]")
	a.drainUntilRevision(6)

	a.send("A5 EXPUNGE\r\n")
	// Ascending current sequence positions: UID1=1, UID2=2, UID3=3.
	a.expectLine("* 1 EXPUNGE")
	a.expectLine("* 2 EXPUNGE")
	a.expectLine("* 1 EXISTS")
	a.expectContains("[REVISION 7]")
	notices = b.drainUntilRevision(7)
	if !containsLine(notices, "* 1 EXPUNGE") || !containsLine(notices, "* 2 EXPUNGE") || !containsLine(notices, "* 1 EXISTS") {
		t.Fatalf("B expunge notices = %#v", notices)
	}

	// UIDs remain identity after sequence renumbering. Remaining UID 3 is seq 1.
	a.send("A6 UID FETCH 3 (UID FLAGS BODY[])\r\n")
	a.expectLine("* 1 FETCH (UID 3 FLAGS () BODY[] {5}")
	if got := readBody(a, 5); got != "third" {
		t.Fatalf("UID 3 body = %q", got)
	}
	a.expectLine(")")
	a.expectStatus("A6", "OK")

	a.send("A7 UID FETCH 1:2 (UID FLAGS)\r\n")
	a.expectStatus("A7", "OK")

	// New append gets UID 4, proving deleted UIDs 1 and 2 are never reused.
	a.appendMsg("A8", "INBOX", []byte("fourth"))
	a.expectLine("* 2 EXISTS")
	a.expectContains("[APPENDUID 1 4] [REVISION 8]")
	b.drainUntilRevision(8)
}

func TestFragmentedAndBackToBackCommands(t *testing.T) {
	srv := startTestServer(t)
	c := dialClient(t, srv.addr)
	body := []byte("line\r\nA1 BAD command text inside body\r\n{7}")

	// Send one APPEND one byte at a time; the body length, not newline text,
	// determines where the payload ends.
	header := fmt.Appendf(nil, "T1 APPEND INBOX {%d}\r\n", len(body))
	packet := append(header, body...)
	packet = append(packet, "\r\nT2 SELECT INBOX\r\nT3 UID FETCH 1 (UID FLAGS BODY[])\r\n"...)
	for _, b := range packet {
		c.sendRaw([]byte{b})
	}

	c.expectLine("* 1 EXISTS")
	c.expectContains("T1 OK [APPENDUID 1 1] [REVISION 1]")
	c.expectLine("* 1 EXISTS")
	c.expectContains("[UIDVALIDITY")
	c.expectContains("[UIDNEXT 2]")
	c.expectContains("[REVISION 1]")
	c.expectStatus("T2", "OK")
	c.expectLine("* 1 FETCH (UID 1 FLAGS () BODY[] {" + strconv.Itoa(len(body)) + "}")
	if got := readBody(c, len(body)); got != string(body) {
		t.Fatalf("body mismatch: %q", got)
	}
	c.expectLine(")")
	c.expectStatus("T3", "OK")
}

func TestLimitsAndPartialAppend(t *testing.T) {
	srv := startTestServer(t)
	c := dialClient(t, srv.addr)

	oversize := make([]byte, maxMessageBytes+1)
	c.appendMsg("L1", "INBOX", oversize)
	c.expectContains("L1 NO message exceeds 16 KiB limit")

	valid := make([]byte, maxMessageBytes)
	c.appendMsg("L2", "INBOX", valid)
	c.expectLine("* 1 EXISTS")
	c.expectContains("[APPENDUID 1 1] [REVISION 1]")

	c.send("L3 SELECT INBOX\r\n")
	c.expectLine("* 1 EXISTS")
	c.expectContains("[UIDVALIDITY 1]")
	c.expectContains("[UIDNEXT 2]")
	c.expectContains("[REVISION 1]")
	c.expectStatus("L3", "OK")

	// Fill the mailbox, but avoid expensive per-message setup overhead by using
	// the same TCP stream for the remaining 199 messages.
	for i := 2; i <= maxMessages; i++ {
		tag := fmt.Sprintf("F%03d", i)
		c.appendMsg(tag, "INBOX", []byte("x"))
	}
	for i := 2; i <= maxMessages; i++ {
		tag := fmt.Sprintf("F%03d", i)
		c.expectLine(fmt.Sprintf("* %d EXISTS", i))
		c.expectContains(tag + " OK")
	}
	c.appendMsg("L4", "INBOX", []byte("overflow"))
	c.expectContains("L4 NO mailbox full")

	// A separate client cannot create a 201st message in that mailbox.
	d := dialClient(t, srv.addr)
	d.appendMsg("D1", "INBOX", []byte("overflow"))
	d.expectContains("D1 NO mailbox full")
	_ = c
}

func TestDisconnectDuringLiteralLeavesNoMessage(t *testing.T) {
	srv := startTestServer(t)
	c := dialClient(t, srv.addr)
	c.sendRaw([]byte("X1 APPEND INBOX {11}\r\npartial"))
	// Close before all 11 declared bytes and terminator arrive.
	c.conn.Close()

	// The message is only inserted after all declared bytes are read, so the
	// mailbox must never have existed once the handler observes the EOF.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := srv.srv.GetMailbox("INBOX"); err == ErrNoSuchMailbox {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("partial append left a mailbox/message behind")
}

func containsLine(lines []string, want string) bool {
	for _, line := range lines {
		if line == want {
			return true
		}
	}
	return false
}
