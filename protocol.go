package main

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

type fetchRequest struct {
	uids      []int64
	wantUID   bool
	wantFlags bool
	wantBody  bool
}

func (s *Session) handleCommand(line string, literal []byte) (bool, error) {
	fields, err := tokenize(line)
	if err != nil {
		return false, err
	}
	if len(fields) == 0 {
		return false, fmt.Errorf("empty command")
	}
	tag := fields[0]
	if !validTag(tag) {
		return false, fmt.Errorf("invalid command tag")
	}
	if len(fields) == 1 {
		return false, fmt.Errorf("missing command")
	}

	cmd := strings.ToUpper(fields[1])
	switch cmd {
	case "NOOP":
		s.reply(tag, "OK", "NOOP completed")
	case "LOGOUT":
		s.reply(tag, "BYE", "logging out")
		return true, nil
	case "SELECT":
		if err := s.selectCommand(tag, fields[2:]); err != nil {
			s.reply(tag, "NO", err.Error())
		}
	case "APPEND":
		if err := s.appendCommand(tag, fields[2:], literal); err != nil {
			s.reply(tag, "NO", err.Error())
		}
	case "EXPUNGE":
		if len(fields) != 2 {
			return false, fmt.Errorf("EXPUNGE takes no arguments")
		}
		if err := s.expungeCommand(tag); err != nil {
			s.reply(tag, "NO", err.Error())
		}
	case "UID":
		if len(fields) < 3 {
			return false, fmt.Errorf("missing UID subcommand")
		}
		switch strings.ToUpper(fields[2]) {
		case "MOVE":
			if err := s.uidMoveCommand(tag, fields[3:]); err != nil {
				s.reply(tag, "NO", err.Error())
			}
		case "FETCH":
			if err := s.uidFetchCommand(tag, fields[3:]); err != nil {
				s.reply(tag, "NO", err.Error())
			}
		case "STORE":
			if err := s.uidStoreCommand(tag, fields[3:]); err != nil {
				s.reply(tag, "NO", err.Error())
			}
		default:
			return false, fmt.Errorf("unsupported UID subcommand")
		}
	default:
		return false, fmt.Errorf("unsupported command")
	}
	return false, nil
}

func (s *Session) reply(tag, status, text string) {
	s.enqueue([]byte(tagToken(tag) + " " + status + " " + sanitizeText(text) + "\r\n"))
}

func (s *Session) selectedMailbox() (string, bool) {
	s.mailboxMu.Lock()
	defer s.mailboxMu.Unlock()
	return s.mailbox, s.mailbox != ""
}

func (s *Session) selectCommand(tag string, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("SELECT requires one mailbox")
	}
	name, err := mailboxToken(args[0])
	if err != nil {
		return err
	}

	hub := s.server.HubFor(name)
	hub.mu.Lock()
	info, err := s.server.GetMailbox(name)
	if err != nil {
		hub.mu.Unlock()
		return err
	}
	// Subscribe while holding the mailbox revision lock and send the snapshot
	// in that same critical section. A committed mutation either happens before
	// the snapshot (included there) or after subscription (sent as an event).
	hub.subscribeLocked(s)
	s.mailboxMu.Lock()
	old := s.mailbox
	s.mailbox = info.Name
	s.mailboxMu.Unlock()

	var frame bytes.Buffer
	fmt.Fprintf(&frame, "* %d EXISTS\r\n", info.Exists)
	fmt.Fprintf(&frame, "* OK [UIDVALIDITY %d] permanent identifiers valid\r\n", info.UIDValidity)
	fmt.Fprintf(&frame, "* OK [UIDNEXT %d] next append UID\r\n", info.UIDNext)
	fmt.Fprintf(&frame, "* OK [REVISION %d] current mailbox revision\r\n", info.Revision)
	fmt.Fprintf(&frame, "%s OK SELECT completed\r\n", tagToken(tag))
	s.enqueue(frame.Bytes())
	hub.mu.Unlock()

	if old != "" && old != info.Name {
		s.server.HubFor(old).unsubscribe(s)
	}
	return nil
}

func (s *Session) appendCommand(tag string, args []string, literal []byte) error {
	if len(args) < 1 {
		return fmt.Errorf("APPEND requires a mailbox and literal")
	}
	name, err := mailboxToken(args[0])
	if err != nil {
		return err
	}
	var flags []string
	rest := args[1:]
	if len(rest) > 0 && strings.HasPrefix(rest[0], "(") {
		if !strings.HasSuffix(rest[0], ")") {
			return fmt.Errorf("malformed flag list")
		}
		inner := rest[0][1 : len(rest[0])-1]
		if inner != "" {
			flags = strings.Split(inner, " ")
			for _, flag := range flags {
				if !validFlag(flag) {
					return fmt.Errorf("invalid message flag %q", flag)
				}
			}
		}
		rest = rest[1:]
	}
	// An optional IMAP date-time is intentionally accepted and ignored.
	if len(rest) == 1 && !strings.HasPrefix(rest[0], "{") {
		rest = rest[:0]
	}
	if len(rest) != 0 || literal == nil {
		return fmt.Errorf("APPEND syntax: APPEND mailbox (flags) {length}")
	}

	result, info, _, err := s.server.AppendMessage(s, name, literal, flags)
	if err != nil {
		return err
	}
	var frame bytes.Buffer
	fmt.Fprintf(&frame, "* %d EXISTS\r\n", info.Exists)
	fmt.Fprintf(&frame, "%s OK [APPENDUID %d %d] [REVISION %d] APPEND completed\r\n",
		tagToken(tag), info.UIDValidity, result.UID, result.Revision)
	s.enqueue(frame.Bytes())
	return nil
}

func (s *Session) expungeCommand(tag string) error {
	name, ok := s.selectedMailbox()
	if !ok {
		return fmt.Errorf("no mailbox selected")
	}
	result, info, err := s.server.ExpungeDeleted(s, name)
	if err != nil {
		return err
	}
	var frame bytes.Buffer
	for _, seq := range result.SeqNums {
		fmt.Fprintf(&frame, "* %d EXPUNGE\r\n", seq)
	}
	if len(result.SeqNums) > 0 {
		fmt.Fprintf(&frame, "* %d EXISTS\r\n", info.Exists)
		fmt.Fprintf(&frame, "%s OK [REVISION %d] EXPUNGE completed\r\n", tagToken(tag), result.Revision)
	} else {
		fmt.Fprintf(&frame, "%s OK EXPUNGE completed\r\n", tagToken(tag))
	}
	s.enqueue(frame.Bytes())
	return nil
}

func (s *Session) uidFetchCommand(tag string, args []string) error {
	req, err := parseFetch(args)
	if err != nil {
		return err
	}
	name, ok := s.selectedMailbox()
	if !ok {
		return fmt.Errorf("no mailbox selected")
	}
	messages, err := s.server.FetchMessages(name, req.uids, req.wantBody)
	if err != nil {
		return err
	}
	var frame bytes.Buffer
	for _, msg := range messages {
		writeFetchResponse(&frame, msg, req)
	}
	fmt.Fprintf(&frame, "%s OK UID FETCH completed\r\n", tagToken(tag))
	s.enqueue(frame.Bytes())
	return nil
}

func (s *Session) uidStoreCommand(tag string, args []string) error {
	if len(args) < 3 {
		return fmt.Errorf("UID STORE requires UID set, operation and flags")
	}
	uids, err := parseUIDSet(args[0])
	if err != nil {
		return err
	}
	operation := strings.ToUpper(args[1])
	silent := false
	if strings.HasSuffix(operation, ".SILENT") {
		silent = true
		operation = strings.TrimSuffix(operation, ".SILENT")
	}
	var action flagAction
	switch operation {
	case "FLAGS":
		action = flagSet
	case "+FLAGS":
		action = flagAdd
	case "-FLAGS":
		action = flagRemove
	default:
		return fmt.Errorf("unsupported STORE operation")
	}
	if len(args) != 3 || !strings.HasPrefix(args[2], "(") || !strings.HasSuffix(args[2], ")") {
		return fmt.Errorf("STORE requires a parenthesized flag list")
	}
	inner := args[2][1 : len(args[2])-1]
	var flags []string
	if inner != "" {
		flags = strings.Split(inner, " ")
	}
	for _, flag := range flags {
		if !validFlag(flag) {
			return fmt.Errorf("invalid message flag %q", flag)
		}
	}
	name, ok := s.selectedMailbox()
	if !ok {
		return fmt.Errorf("no mailbox selected")
	}
	result, _, err := s.server.StoreFlags(s, name, uids, action, flags, silent)
	if err != nil {
		return err
	}
	var frame bytes.Buffer
	if !silent {
		for _, msg := range result.Messages {
			writeFetchResponse(&frame, msg, fetchRequest{wantUID: true, wantFlags: true})
		}
	}
	if result.Revision != 0 {
		fmt.Fprintf(&frame, "%s OK [REVISION %d] UID STORE completed\r\n", tagToken(tag), result.Revision)
	} else {
		fmt.Fprintf(&frame, "%s OK UID STORE completed\r\n", tagToken(tag))
	}
	s.enqueue(frame.Bytes())
	return nil
}

func parseFetch(args []string) (fetchRequest, error) {
	var req fetchRequest
	if len(args) < 2 {
		return req, fmt.Errorf("UID FETCH requires UID set and fetch items")
	}
	var err error
	if req.uids, err = parseUIDSet(args[0]); err != nil {
		return req, err
	}
	items := strings.ToUpper(strings.Join(args[1:], " "))
	req.wantUID = true
	tokens := strings.FieldsFunc(items, func(r rune) bool {
		return r == ' ' || r == '(' || r == ')'
	})
	for _, token := range tokens {
		switch {
		case token == "FLAGS":
			req.wantFlags = true
		case token == "ALL":
			req.wantFlags = true
		case strings.HasPrefix(token, "BODY"):
			req.wantBody = true
		}
	}
	if !req.wantBody && !req.wantFlags {
		return req, fmt.Errorf("unsupported FETCH items; use UID, FLAGS, BODY[] or ALL")
	}
	return req, nil
}

func writeFetchResponse(b *bytes.Buffer, msg MessageInfo, req fetchRequest) {
	var parts []string
	if req.wantUID {
		parts = append(parts, "UID "+strconv.FormatInt(msg.UID, 10))
	}
	if req.wantFlags {
		parts = append(parts, "FLAGS ("+formatFlags(msg.Flags)+")")
	}
	fmt.Fprintf(b, "* %d FETCH (%s", msg.Seq, strings.Join(parts, " "))
	if req.wantBody {
		if len(parts) > 0 {
			b.WriteByte(' ')
		}
		fmt.Fprintf(b, "BODY[] {%d}\r\n", len(msg.Body))
		b.Write(msg.Body)
		b.WriteString("\r\n")
	}
	b.WriteString(")\r\n")
}

func parseUIDSet(text string) ([]int64, error) {
	if text == "*" {
		return nil, nil
	}
	parts := strings.Split(text, ",")
	var uids []int64
	seen := map[int64]bool{}
	for _, part := range parts {
		if part == "" {
			return nil, fmt.Errorf("empty UID in set")
		}
		if strings.Contains(part, ":") {
			bounds := strings.Split(part, ":")
			if len(bounds) != 2 {
				return nil, fmt.Errorf("invalid UID range")
			}
			start, err := strconv.ParseInt(bounds[0], 10, 64)
			if err != nil || start <= 0 {
				return nil, fmt.Errorf("invalid UID range")
			}
			if bounds[1] == "*" {
				uids = append(uids, start)
				continue
			}
			end, err := strconv.ParseInt(bounds[1], 10, 64)
			if err != nil || end < start {
				return nil, fmt.Errorf("invalid UID range")
			}
			for uid := start; uid <= end; uid++ {
				if !seen[uid] {
					uids = append(uids, uid)
					seen[uid] = true
				}
			}
		} else {
			uid, err := strconv.ParseInt(part, 10, 64)
			if err != nil || uid <= 0 {
				return nil, fmt.Errorf("invalid UID %q", part)
			}
			if !seen[uid] {
				uids = append(uids, uid)
				seen[uid] = true
			}
		}
	}
	return uids, nil
}

func (s *Session) uidMoveCommand(tag string, args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("UID MOVE requires UID set and destination")
	}
	uids, err := parseUIDSet(args[0])
	if err != nil {
		return err
	}
	destination, err := mailboxToken(args[1])
	if err != nil {
		return err
	}
	source, ok := s.selectedMailbox()
	if !ok {
		return fmt.Errorf("no mailbox selected")
	}
	result, err := s.server.MoveMessages(s, source, destination, uids)
	if err != nil {
		return err
	}
	var frame bytes.Buffer
	frame.Write(renderEvent(result.SourceEvent))
	oldIDs, newIDs := []string{}, []string{}
	for _, mapping := range result.Mapping {
		oldIDs = append(oldIDs, strconv.FormatInt(mapping.OldUID, 10))
		newIDs = append(newIDs, strconv.FormatInt(mapping.NewUID, 10))
	}
	fmt.Fprintf(&frame, "%s OK [COPYUID %d %s %s] UID MOVE completed\r\n", tagToken(tag), result.DestinationValidity, strings.Join(oldIDs, ","), strings.Join(newIDs, ","))
	s.enqueue(frame.Bytes())
	return nil
}
