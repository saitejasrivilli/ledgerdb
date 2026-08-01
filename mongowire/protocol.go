// Package mongowire implements a deliberately small subset of the real
// MongoDB wire protocol, translating it into calls against docstore
// (v1.0) — see docs/design_wire_protocol.md for scope and rationale.
package mongowire

import (
	"encoding/binary"
	"fmt"
	"io"

	"go.mongodb.org/mongo-driver/v2/bson"
)

const (
	opReply = 1
	opQuery = 2004
	opMsg   = 2013
)

// header is the 16-byte MongoDB wire protocol message header.
type header struct {
	MessageLength int32
	RequestID     int32
	ResponseTo    int32 // set to the request's RequestID on replies
	OpCode        int32
}

// readMessage reads one wire protocol request off r and returns the
// merged command document plus which opcode it arrived as — modern
// drivers send every real command over OP_MSG, but the very FIRST
// handshake on a fresh connection goes out as the legacy OP_QUERY
// (opcode 2004) against "$cmd", since the driver doesn't yet know this
// server understands OP_MSG. A server that only understands OP_MSG never
// gets past that first handshake with a real driver — this is why
// opQuery has to be handled too, not just opMsg. Kind-1 document-sequence
// sections (OP_MSG only) are merged into the command as array fields
// keyed by their identifier, the standard server-side handling since
// drivers commonly send bulk arrays (e.g. insert's "documents") that way
// rather than embedded in the kind-0 body.
func readMessage(r io.Reader) (reqID int32, cmd bson.M, opCode int32, err error) {
	var hdr header
	if err := binary.Read(r, binary.LittleEndian, &hdr); err != nil {
		return 0, nil, 0, err
	}

	switch hdr.OpCode {
	case opQuery:
		cmd, err := readQuery(r, hdr)
		return hdr.RequestID, cmd, opQuery, err
	case opMsg:
		cmd, err := readOpMsgBody(r, hdr)
		return hdr.RequestID, cmd, opMsg, err
	default:
		return 0, nil, 0, fmt.Errorf("mongowire: unsupported opcode %d", hdr.OpCode)
	}
}

// readQuery parses a legacy OP_QUERY body — flags, full collection name,
// numberToSkip/numberToReturn, then the query document. Only used for
// the initial handshake (see readMessage's doc comment); this version
// doesn't support OP_QUERY for anything beyond that.
func readQuery(r io.Reader, hdr header) (bson.M, error) {
	body := make([]byte, hdr.MessageLength-16)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, err
	}
	pos := 4 // flags
	for body[pos] != 0 {
		pos++
	}
	pos++        // skip the collection name's null terminator
	pos += 4 * 2 // numberToSkip, numberToReturn

	doc, _, err := readRawDoc(body, pos)
	if err != nil {
		return nil, err
	}
	var m bson.M
	if err := bson.Unmarshal(doc, &m); err != nil {
		return nil, fmt.Errorf("mongowire: decode OP_QUERY doc: %w", err)
	}
	return m, nil
}

func readOpMsgBody(r io.Reader, hdr header) (bson.M, error) {
	body := make([]byte, hdr.MessageLength-16)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, err
	}

	pos := 0
	// flagBits (uint32) — ignored: none of the flags this version cares
	// about (checksumPresent, moreToCome) change command dispatch.
	pos += 4

	cmd := bson.M{}
	for pos < len(body) {
		kind := body[pos]
		pos++
		switch kind {
		case 0:
			var doc bson.Raw
			var err error
			doc, pos, err = readRawDoc(body, pos)
			if err != nil {
				return nil, err
			}
			var m bson.M
			if err := bson.Unmarshal(doc, &m); err != nil {
				return nil, fmt.Errorf("mongowire: decode kind-0 section: %w", err)
			}
			for k, v := range m {
				cmd[k] = v
			}
		case 1:
			seqLen := int32(binary.LittleEndian.Uint32(body[pos:]))
			seqEnd := pos + int(seqLen)
			p := pos + 4
			nameEnd := p
			for body[nameEnd] != 0 {
				nameEnd++
			}
			identifier := string(body[p:nameEnd])
			p = nameEnd + 1

			var docs []interface{}
			for p < seqEnd {
				var doc bson.Raw
				var err error
				doc, p, err = readRawDoc(body, p)
				if err != nil {
					return nil, err
				}
				var m bson.M
				if err := bson.Unmarshal(doc, &m); err != nil {
					return nil, fmt.Errorf("mongowire: decode kind-1 doc: %w", err)
				}
				docs = append(docs, m)
			}
			cmd[identifier] = docs
			pos = seqEnd
		default:
			return nil, fmt.Errorf("mongowire: unsupported OP_MSG section kind %d", kind)
		}
	}

	return cmd, nil
}

func readRawDoc(body []byte, pos int) (bson.Raw, int, error) {
	if pos+4 > len(body) {
		return nil, 0, fmt.Errorf("mongowire: truncated document length")
	}
	docLen := int32(binary.LittleEndian.Uint32(body[pos:]))
	end := pos + int(docLen)
	if end > len(body) {
		return nil, 0, fmt.Errorf("mongowire: truncated document body")
	}
	return bson.Raw(body[pos:end]), end, nil
}

// writeReply replies in whichever opcode the request arrived as — an
// OP_QUERY handshake must get an OP_REPLY back (a driver won't accept an
// OP_MSG reply to its legacy handshake), everything else replies OP_MSG.
func writeReply(w io.Writer, requestOpCode, responseTo int32, reply bson.M) error {
	if requestOpCode == opQuery {
		return writeOpReply(w, responseTo, reply)
	}
	return writeOpMsg(w, responseTo, reply)
}

// writeOpMsg writes an OP_MSG reply containing a single kind-0 section
// with reply as its body — every OP_MSG response this version sends is a
// single document, no kind-1 sequences needed on the reply side.
func writeOpMsg(w io.Writer, responseTo int32, reply bson.M) error {
	docBytes, err := bson.Marshal(reply)
	if err != nil {
		return fmt.Errorf("mongowire: marshal reply: %w", err)
	}

	body := make([]byte, 0, 5+len(docBytes))
	body = append(body, 0, 0, 0, 0) // flagBits
	body = append(body, 0)          // section kind 0
	body = append(body, docBytes...)

	hdr := header{
		MessageLength: int32(16 + len(body)),
		RequestID:     0,
		ResponseTo:    responseTo,
		OpCode:        opMsg,
	}
	if err := binary.Write(w, binary.LittleEndian, hdr); err != nil {
		return err
	}
	_, err = w.Write(body)
	return err
}

// writeOpReply writes a legacy OP_REPLY — used only for the initial
// OP_QUERY handshake response (see readMessage's doc comment).
func writeOpReply(w io.Writer, responseTo int32, reply bson.M) error {
	docBytes, err := bson.Marshal(reply)
	if err != nil {
		return fmt.Errorf("mongowire: marshal reply: %w", err)
	}

	body := make([]byte, 0, 20+len(docBytes))
	body = append(body, 0, 0, 0, 0)             // responseFlags
	body = append(body, 0, 0, 0, 0, 0, 0, 0, 0) // cursorID (int64) = 0
	body = append(body, 0, 0, 0, 0)             // startingFrom
	body = append(body, 1, 0, 0, 0)             // numberReturned = 1
	body = append(body, docBytes...)

	hdr := header{
		MessageLength: int32(16 + len(body)),
		RequestID:     0,
		ResponseTo:    responseTo,
		OpCode:        opReply,
	}
	if err := binary.Write(w, binary.LittleEndian, hdr); err != nil {
		return err
	}
	_, err = w.Write(body)
	return err
}
