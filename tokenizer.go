package main

import (
	"fmt"
	"strings"
	"unicode"
)

func tokenize(line string) ([]string, error) {
	var tokens []string
	var current strings.Builder
	flush := func() {
		if current.Len() > 0 {
			tokens = append(tokens, current.String())
			current.Reset()
		}
	}

	for i := 0; i < len(line); {
		c := line[i]
		switch {
		case c == ' ' || c == '\t':
			flush()
			i++
		case c == '"':
			i++
			for i < len(line) {
				if line[i] == '\\' && i+1 < len(line) {
					current.WriteByte(line[i+1])
					i += 2
					continue
				}
				if line[i] == '"' {
					break
				}
				current.WriteByte(line[i])
				i++
			}
			if i >= len(line) {
				return nil, fmt.Errorf("unterminated quoted string")
			}
			i++
			flush()
		case c == '(':
			end := strings.IndexByte(line[i:], ')')
			if end < 0 {
				return nil, fmt.Errorf("unterminated parenthesized list")
			}
			current.WriteString(line[i : i+end+1])
			i += end + 1
			flush()
		case c == '{':
			// The literal length marker belongs to the already parsed command
			// line; the body itself is read by readCommand.
			end := strings.IndexByte(line[i:], '}')
			if end < 0 {
				return nil, fmt.Errorf("unterminated literal marker")
			}
			current.WriteString(line[i : i+end+1])
			i += end + 1
			flush()
		default:
			for i < len(line) && line[i] != ' ' && line[i] != '\t' && line[i] != '(' && line[i] != '{' {
				current.WriteByte(line[i])
				i++
			}
		}
	}
	flush()
	return tokens, nil
}

func validTag(tag string) bool {
	if tag == "" || len(tag) > 32 {
		return false
	}
	for _, r := range tag {
		if r > unicode.MaxASCII {
			return false
		}
		if strings.ContainsRune("(){%*\"\\]", r) {
			return false
		}
	}
	return true
}

func tagToken(tag string) string {
	if validTag(tag) {
		return tag
	}
	return "*"
}

func mailboxToken(token string) (string, error) {
	if token == "" || strings.ContainsAny(token, "\r\n") {
		return "", fmt.Errorf("invalid mailbox name")
	}
	return canonicalMailbox(token), nil
}

func validFlag(flag string) bool {
	if flag == "" || len(flag) > 64 || strings.ContainsAny(flag, "\r\n()") {
		return false
	}
	if strings.HasPrefix(flag, `\`) {
		for _, r := range flag[1:] {
			if !unicode.IsLetter(r) && r != '_' {
				return false
			}
		}
	}
	return true
}

func sanitizeText(text string) string {
	text = strings.ReplaceAll(text, "\r", " ")
	text = strings.ReplaceAll(text, "\n", " ")
	if len(text) > 256 {
		text = text[:256]
	}
	return text
}
