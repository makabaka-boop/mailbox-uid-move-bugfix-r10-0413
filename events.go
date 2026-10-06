package main

import (
	"bytes"
	"fmt"
)

func renderEvent(ev Event) []byte {
	var b bytes.Buffer
	switch ev.Type {
	case EventAppend:
		fmt.Fprintf(&b, "* %d EXISTS\r\n", ev.Exists)
		fmt.Fprintf(&b, "* APPENDUID %d %d\r\n", ev.UIDValidity, ev.UID)
		fmt.Fprintf(&b, "* REVISION %d\r\n", ev.Revision)
	case EventMoveIn:
		fmt.Fprintf(&b, "* %d EXISTS\r\n", ev.Exists)
		for _, uid := range ev.UIDs {
			fmt.Fprintf(&b, "* APPENDUID %d %d\r\n", ev.UIDValidity, uid)
		}
		fmt.Fprintf(&b, "* REVISION %d\r\n", ev.Revision)
	case EventMoveOut:
		// One EXPUNGE per removed message, sequence numbers as of before the
		// whole batch, accounting for earlier removals in the same operation.
		for _, seq := range ev.SeqNums {
			fmt.Fprintf(&b, "* %d EXPUNGE\r\n", seq)
		}
		fmt.Fprintf(&b, "* %d EXISTS\r\n", ev.Exists)
		fmt.Fprintf(&b, "* REVISION %d\r\n", ev.Revision)
	case EventStore:
		for _, msg := range ev.Messages {
			fmt.Fprintf(&b, "* %d FETCH (UID %d FLAGS (%s))\r\n",
				msg.Seq, msg.UID, formatFlags(msg.Flags))
		}
		fmt.Fprintf(&b, "* REVISION %d\r\n", ev.Revision)
	case EventExpunge:
		for _, seq := range ev.SeqNums {
			fmt.Fprintf(&b, "* %d EXPUNGE\r\n", seq)
		}
		fmt.Fprintf(&b, "* %d EXISTS\r\n", ev.Exists)
		fmt.Fprintf(&b, "* REVISION %d\r\n", ev.Revision)
	}
	return b.Bytes()
}
