package main

import (
	"strconv"
	"strings"
	"sync"
	"testing"
)

// consumeSnapshot reads the five lines of a SELECT response.
func (c *testClient) consumeSnapshot() {
	c.t.Helper()
	for range 5 {
		c.line()
	}
}

// fetchBodies returns the set of bodies returned by a UID FETCH over the
// whole mailbox.
func (c *testClient) fetchBodies(tag string, upTo int) map[string]bool {
	c.t.Helper()
	c.send("%s UID FETCH 1:%d (UID FLAGS BODY[])\r\n", tag, upTo)
	bodies := map[string]bool{}
	for {
		line := c.line()
		if strings.HasPrefix(line, tag+" ") {
			return bodies
		}
		idx := strings.IndexByte(line, '{')
		if idx < 0 {
			c.t.Fatalf("unexpected fetch line %q", line)
		}
		size, err := strconv.Atoi(line[idx+1 : len(line)-1])
		if err != nil {
			c.t.Fatal(err)
		}
		bodies[readBody(c, size)] = true
		c.expectLine(")")
	}
}

// selectExists returns the EXISTS count from a fresh SELECT.
func (c *testClient) selectExists(tag, mailbox string) int {
	c.t.Helper()
	c.send("%s SELECT %s\r\n", tag, mailbox)
	line := c.expectContains(" EXISTS")
	parts := strings.SplitN(line, " ", 3)
	n, err := strconv.Atoi(parts[1])
	if err != nil {
		c.t.Fatalf("bad EXISTS line %q: %v", line, err)
	}
	for range 4 {
		c.line()
	}
	return n
}

// TestUIDMoveConcurrentBothDirections hammers hub lock pairs concurrently.
// Messages move from ALPHA and BETA into GAMMA simultaneously: moves acquire
// (ALPHA,GAMMA) and (BETA,GAMMA) pairs, while the deterministic lexicographic
// lock order must serialize them without deadlock. Distinct source messages
// mean every requested UID stays present, so all moves must succeed and every
// original body must end up exactly once in GAMMA.
func TestUIDMoveConcurrentBothDirections(t *testing.T) {
	srv := startTestServer(t)
	const batch = 25

	seed := func(tag, mailbox string, from, to int) {
		c := dialClient(t, srv.addr)
		// UIDs are per-mailbox, so both mailboxes allocate UIDs 1..batch even
		// though BETA's bodies carry global numbers batch+1..2*batch.
		for global := from; global <= to; global++ {
			c.appendMsg(tag+strconv.Itoa(global), mailbox, []byte(mailbox+"-"+strconv.Itoa(global)))
			c.expectContains("EXISTS")
			c.expectContains(tag + strconv.Itoa(global) + " OK")
		}
		c.conn.Close()
	}
	seed("A", "ALPHA", 1, batch)
	seed("B", "BETA", batch+1, 2*batch)
	// Create GAMMA with one keeper.
	keeper := dialClient(t, srv.addr)
	keeper.appendMsg("G0", "GAMMA", []byte("keeper"))
	keeper.expectContains("EXISTS")
	keeper.expectContains("G0 OK")
	keeper.conn.Close()

	var wg sync.WaitGroup
	move := func(tag, from, toMailbox string, uid int) {
		defer wg.Done()
		c := dialClient(t, srv.addr)
		c.send("S SELECT %s\r\n", from)
		c.consumeSnapshot()
		c.send("%s UID MOVE %d %s\r\n", tag, uid, toMailbox)
		// Read until the tagged response; distinct source UIDs and a target
		// with ample capacity make success mandatory. EXPUNGE/EXISTS lines
		// precede it, and foreign-source notices never reach this connection.
		for {
			line := c.line()
			if strings.HasPrefix(line, tag+" OK ") {
				return
			}
			if strings.HasPrefix(line, tag+" NO") || strings.HasPrefix(line, tag+" BAD") {
				t.Errorf("%s unexpected failure: %q", tag, line)
				return
			}
		}
	}
	for uid := 1; uid <= batch; uid++ {
		wg.Add(1)
		go move("X"+strconv.Itoa(uid), "ALPHA", "GAMMA", uid)
		wg.Add(1)
		go move("Y"+strconv.Itoa(uid), "BETA", "GAMMA", uid)
	}
	wg.Wait()

	alpha := dialClient(t, srv.addr)
	if n := alpha.selectExists("K0", "ALPHA"); n != 0 {
		t.Fatalf("ALPHA EXISTS = %d, want 0", n)
	}
	beta := dialClient(t, srv.addr)
	if n := beta.selectExists("K2", "BETA"); n != 0 {
		t.Fatalf("BETA EXISTS = %d, want 0", n)
	}

	gamma := dialClient(t, srv.addr)
	gammaExists := gamma.selectExists("K4", "GAMMA")
	if gammaExists != 2*batch+1 {
		t.Fatalf("GAMMA EXISTS = %d, want %d", gammaExists, 2*batch+1)
	}
	bodies := gamma.fetchBodies("K5", 3*batch)
	if bodies["keeper"] != true {
		t.Fatal("keeper missing after concurrent moves")
	}
	for uid := 1; uid <= 2*batch; uid++ {
		body := "ALPHA-"
		if uid > batch {
			body = "BETA-"
		}
		body += strconv.Itoa(uid)
		if !bodies[body] {
			t.Fatalf("body %q missing after concurrent moves", body)
		}
	}
	if len(bodies) != 2*batch+1 {
		t.Fatalf("GAMMA bodies = %d, want %d", len(bodies), 2*batch+1)
	}
}
