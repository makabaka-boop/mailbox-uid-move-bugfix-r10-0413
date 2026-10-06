package main

import (
	"sort"
	"strings"
)

type flagAction int

const (
	flagSet flagAction = iota
	flagAdd
	flagRemove
)

func canonicalMailbox(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}

func canonicalFlag(flag string) string {
	flag = strings.TrimSpace(flag)
	if strings.HasPrefix(flag, `\`) {
		return strings.ToLower(flag)
	}
	return flag
}

func flagMap(flags []string) map[string]string {
	m := make(map[string]string)
	for _, flag := range flags {
		flag = strings.TrimSpace(flag)
		if flag != "" {
			m[canonicalFlag(flag)] = flag
		}
	}
	return m
}

// Flags are stored one token per line, a deliberately simple reversible
// encoding. Payload bytes never enter this table.
func encodeFlags(flags []string) string {
	return encodeMapFlags(flagMap(flags))
}

func encodeMapFlags(flags map[string]string) string {
	if len(flags) == 0 {
		return ""
	}
	keys := make([]string, 0, len(flags))
	for key := range flags {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	values := make([]string, 0, len(keys))
	for _, key := range keys {
		values = append(values, flags[key])
	}
	return strings.Join(values, "\n")
}

func decodeFlags(encoded string) []string {
	if encoded == "" {
		return nil
	}
	parts := strings.Split(encoded, "\n")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func formatFlags(flags []string) string {
	return strings.Join(flags, " ")
}
