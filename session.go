package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// errOversizeLiteral is non-fatal: the declared bytes and CRLF have already
// been consumed, so the tagged NO leaves the command stream synchronized.
var errOversizeLiteral = errors.New("message exceeds 16 KiB limit")

// Declared literals larger than this hard cap close the connection rather
// than forcing the server to drain an arbitrarily large amount of data.
const maxDeclaredLiteral = 1 << 20

type Session struct {
	server *Server
	conn   net.Conn
	id     uint64

	out     chan []byte
	closing chan struct{}
	done    chan struct{}
	closeMu sync.Mutex
	closed  bool

	mailbox   string
	mailboxMu sync.Mutex
}

var sessionID uint64

func NewSession(server *Server, conn net.Conn) *Session {
	return &Session{
		server: server,
		conn:   conn,
		id:     atomic.AddUint64(&sessionID, 1),
		// Large enough to buffer every notification a 200-message mailbox can
		// produce (three frames per append) without a slow reader stalling
		// mailbox-wide revisions; overflow still disconnects the slow client.
		out:     make(chan []byte, 4*maxMessages),
		closing: make(chan struct{}),
		done:    make(chan struct{}),
	}
}

func (s *Session) Serve() {
	defer close(s.done)
	go s.writeLoop()
	defer func() {
		_ = s.conn.Close()
		s.setSelected("")
		s.markClosed()
	}()
	s.setSelected("")
	s.enqueue([]byte("* OK custom mailbox service ready\r\n"))

	reader := bufio.NewReaderSize(s.conn, 32*1024)
	for {
		line, literal, err := readCommand(reader)
		if err != nil {
			if errors.Is(err, errOversizeLiteral) {
				tag := firstToken(line)
				if !validTag(tag) {
					tag = "*"
				}
				s.enqueue([]byte(tagToken(tag) + " NO " + errOversizeLiteral.Error() + "\r\n"))
				continue
			}
			if !errors.Is(err, io.EOF) && !isClosedNetErr(err) && !errors.Is(err, io.ErrUnexpectedEOF) {
				s.enqueue([]byte("* BAD " + sanitizeText(err.Error()) + "\r\n"))
			}
			return
		}
		terminate, err := s.handleCommand(line, literal)
		if err != nil {
			tag := firstToken(line)
			if !validTag(tag) {
				tag = "*"
			}
			s.enqueue([]byte(tagToken(tag) + " BAD " + sanitizeText(err.Error()) + "\r\n"))
			return
		}
		if terminate {
			return
		}
	}
}

func (s *Session) writeLoop() {
	for {
		select {
		case frame := <-s.out:
			if _, err := s.conn.Write(frame); err != nil {
				_ = s.conn.Close()
				return
			}
		case <-s.closing:
			return
		}
	}
}

func (s *Session) markClosed() {
	s.closeMu.Lock()
	if !s.closed {
		s.closed = true
		close(s.closing)
	}
	s.closeMu.Unlock()
}

func (s *Session) Close() {
	_ = s.conn.Close()
	<-s.done
}

func (s *Session) Write(p []byte) (int, error) {
	if !s.enqueue(append([]byte(nil), p...)) {
		return 0, net.ErrClosed
	}
	return len(p), nil
}

func (s *Session) enqueue(frame []byte) bool {
	s.closeMu.Lock()
	if s.closed {
		s.closeMu.Unlock()
		return false
	}
	select {
	case s.out <- frame:
		s.closeMu.Unlock()
		return true
	case <-s.closing:
		s.closeMu.Unlock()
		return false
	default:
		// Publisher must not block a committed mailbox revision.
		s.closeMu.Unlock()
		go s.Close()
		return false
	}
}

func (s *Session) setSelected(name string) {
	s.mailboxMu.Lock()
	old := s.mailbox
	s.mailbox = name
	s.mailboxMu.Unlock()
	if old != "" {
		s.server.HubFor(old).unsubscribe(s)
	}
}

// readCommand reads one complete command. For APPEND, the command line ends
// with {N}; N body bytes plus the following CRLF are consumed before command
// execution. This also works when the header/body arrive in many TCP segments
// or when several commands are already buffered back-to-back.
func readCommand(reader *bufio.Reader) (line string, literal []byte, err error) {
	line, err = readLine(reader)
	if err != nil {
		return "", nil, err
	}
	fields, parseErr := tokenize(line)
	if parseErr == nil && len(fields) >= 2 && strings.EqualFold(fields[1], "APPEND") {
		last := fields[len(fields)-1]
		if strings.HasPrefix(last, "{") && strings.HasSuffix(last, "}") {
			sizeText := last[1 : len(last)-1]
			size, atoiErr := strconv.Atoi(sizeText)
			if atoiErr != nil || size < 0 || len(sizeText) == 0 {
				return line, nil, fmt.Errorf("invalid literal length")
			}
			if size > maxDeclaredLiteral {
				return line, nil, fmt.Errorf("declared literal too large")
			}
			if size > maxMessageBytes {
				// Consume the full declared literal so the next command stays
				// aligned, then reject this one non-fatally.
				if _, err = io.CopyN(io.Discard, reader, int64(size)); err != nil {
					return "", nil, err
				}
				if err = readCRLF(reader); err != nil {
					return "", nil, err
				}
				return line, nil, errOversizeLiteral
			}
			literal = make([]byte, size)
			if _, err = io.ReadFull(reader, literal); err != nil {
				return "", nil, err
			}
			if err = readCRLF(reader); err != nil {
				return "", nil, err
			}
			fields = fields[:len(fields)-1]
			line = strings.Join(fields, " ")
		}
	}
	return line, literal, nil
}

func readLine(reader *bufio.Reader) (string, error) {
	data, err := reader.ReadBytes('\n')
	if len(data) > 0 {
		if !bytes.HasSuffix(data, []byte("\r\n")) {
			if errors.Is(err, io.EOF) {
				return "", io.ErrUnexpectedEOF
			}
			return "", fmt.Errorf("line must end with CRLF")
		}
		if len(data) > 64*1024 {
			return "", fmt.Errorf("command line too long")
		}
		return string(data[:len(data)-2]), nil
	}
	if errors.Is(err, io.EOF) {
		return "", io.EOF
	}
	return "", err
}

func readCRLF(reader *bufio.Reader) error {
	cr, err := reader.ReadByte()
	if err != nil {
		return err
	}
	if cr != '\r' {
		return fmt.Errorf("expected CRLF after literal")
	}
	lf, err := reader.ReadByte()
	if err != nil {
		return err
	}
	if lf != '\n' {
		return fmt.Errorf("expected CRLF after literal")
	}
	return nil
}

func isClosedNetErr(err error) bool {
	return errors.Is(err, net.ErrClosed) || strings.Contains(err.Error(), "broken pipe")
}

func firstToken(line string) string {
	line = strings.TrimSpace(line)
	if line == "" {
		return ""
	}
	if i := strings.IndexByte(line, ' '); i >= 0 {
		return line[:i]
	}
	return line
}
