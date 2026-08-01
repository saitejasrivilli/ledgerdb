// Package storage implements the segment-based append-only log described
// in docs/design_log_storage.md.
package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
)

const (
	lenPrefixSize  = 4 // uint32 length prefix per record
	indexEntrySize = 8 // uint32 relOffset + uint32 position
	segmentNameFmt = "%020d"
)

// segment is one (baseOffset, .log, .index) triple. baseOffset is the
// offset of the first record stored in this segment.
type segment struct {
	baseOffset int
	nextOffset int
	size       int64
	dir        string

	logFile   *os.File
	indexFile *os.File

	// tiered segments (v0.9) have had their local files deleted after a
	// confirmed upload to a ColdStore, identified by coldKey. Reads fetch
	// fresh from cold storage per call — see docs/design_tiered_storage.md
	// for why this deliberately doesn't cache.
	tiered    bool
	coldKey   string
	coldStore ColdStore
}

func segmentPaths(dir string, baseOffset int) (logPath, indexPath string) {
	name := fmt.Sprintf(segmentNameFmt, baseOffset)
	return filepath.Join(dir, name+".log"), filepath.Join(dir, name+".index")
}

// openSegment opens (creating if needed) the segment starting at baseOffset.
// If the .log file already has content, its index is rebuilt by scanning —
// this is the crash-recovery path described in the design doc, applied
// unconditionally to keep open-segment logic in one place rather than a
// separate "recover" codepath that could drift from the normal one.
func openSegment(dir string, baseOffset int) (*segment, error) {
	logPath, indexPath := segmentPaths(dir, baseOffset)

	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("open log file: %w", err)
	}
	indexFile, err := os.OpenFile(indexPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		logFile.Close()
		return nil, fmt.Errorf("open index file: %w", err)
	}

	s := &segment{
		baseOffset: baseOffset,
		nextOffset: baseOffset,
		dir:        dir,
		logFile:    logFile,
		indexFile:  indexFile,
	}

	if err := s.rebuildIndex(); err != nil {
		logFile.Close()
		indexFile.Close()
		return nil, err
	}
	return s, nil
}

// rebuildIndex scans the .log file from the start and regenerates the
// .index file, recomputing size/nextOffset. Called on every open so the
// index can never be stale or short relative to the log (see crash
// recovery section of the design doc).
func (s *segment) rebuildIndex() error {
	if _, err := s.logFile.Seek(0, 0); err != nil {
		return err
	}
	if _, err := s.indexFile.Seek(0, 0); err != nil {
		return err
	}
	if err := s.indexFile.Truncate(0); err != nil {
		return err
	}

	var pos int64
	lenBuf := make([]byte, lenPrefixSize)
	relOffset := uint32(0)
	for {
		n, err := s.logFile.Read(lenBuf)
		if n == 0 || err != nil {
			break
		}
		recLen := binary.BigEndian.Uint32(lenBuf)
		if err := s.appendIndexEntry(relOffset, uint32(pos)); err != nil {
			return err
		}
		if _, err := s.logFile.Seek(int64(recLen), 1); err != nil {
			return err
		}
		pos += int64(lenPrefixSize) + int64(recLen)
		relOffset++
	}

	s.size = pos
	s.nextOffset = s.baseOffset + int(relOffset)
	_, err := s.logFile.Seek(0, 2) // back to end for appends
	return err
}

func (s *segment) appendIndexEntry(relOffset, position uint32) error {
	buf := make([]byte, indexEntrySize)
	binary.BigEndian.PutUint32(buf[0:4], relOffset)
	binary.BigEndian.PutUint32(buf[4:8], position)
	_, err := s.indexFile.Write(buf)
	return err
}

// append writes payload to the segment, returning the offset assigned to it.
func (s *segment) append(payload []byte) (int, error) {
	lenBuf := make([]byte, lenPrefixSize)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(payload)))

	if _, err := s.logFile.Write(lenBuf); err != nil {
		return 0, err
	}
	if _, err := s.logFile.Write(payload); err != nil {
		return 0, err
	}

	relOffset := uint32(s.nextOffset - s.baseOffset)
	if err := s.appendIndexEntry(relOffset, uint32(s.size)); err != nil {
		return 0, err
	}

	offset := s.nextOffset
	s.size += int64(lenPrefixSize) + int64(len(payload))
	s.nextOffset++
	return offset, nil
}

// read returns the payload stored at the given absolute offset.
func (s *segment) read(offset int) ([]byte, error) {
	if offset < s.baseOffset || offset >= s.nextOffset {
		return nil, fmt.Errorf("offset %d out of range for segment base %d", offset, s.baseOffset)
	}
	relOffset := uint32(offset - s.baseOffset)
	entryPos := int64(relOffset) * indexEntrySize

	if s.tiered {
		return s.readTiered(entryPos)
	}
	return s.readLocal(entryPos)
}

func (s *segment) readLocal(entryPos int64) ([]byte, error) {

	entryBuf := make([]byte, indexEntrySize)
	if _, err := s.indexFile.ReadAt(entryBuf, entryPos); err != nil {
		return nil, fmt.Errorf("read index entry: %w", err)
	}
	position := binary.BigEndian.Uint32(entryBuf[4:8])

	lenBuf := make([]byte, lenPrefixSize)
	if _, err := s.logFile.ReadAt(lenBuf, int64(position)); err != nil {
		return nil, fmt.Errorf("read length prefix: %w", err)
	}
	recLen := binary.BigEndian.Uint32(lenBuf)

	payload := make([]byte, recLen)
	if _, err := s.logFile.ReadAt(payload, int64(position)+lenPrefixSize); err != nil {
		return nil, fmt.Errorf("read payload: %w", err)
	}
	return payload, nil
}

// readTiered fetches this segment's bytes fresh from cold storage (no
// caching, see docs/design_tiered_storage.md) and resolves the read the
// same way a local segment would.
func (s *segment) readTiered(entryPos int64) ([]byte, error) {
	logBytes, indexBytes, err := s.coldStore.Get(s.coldKey)
	if err != nil {
		return nil, fmt.Errorf("fetch tiered segment %s: %w", s.coldKey, err)
	}

	if entryPos+indexEntrySize > int64(len(indexBytes)) {
		return nil, fmt.Errorf("tiered index entry out of range at pos %d", entryPos)
	}
	entryBuf := indexBytes[entryPos : entryPos+indexEntrySize]
	position := int64(binary.BigEndian.Uint32(entryBuf[4:8]))

	if position+lenPrefixSize > int64(len(logBytes)) {
		return nil, fmt.Errorf("tiered log length prefix out of range at pos %d", position)
	}
	recLen := int64(binary.BigEndian.Uint32(logBytes[position : position+lenPrefixSize]))

	start := position + lenPrefixSize
	if start+recLen > int64(len(logBytes)) {
		return nil, fmt.Errorf("tiered payload out of range at pos %d", start)
	}
	payload := make([]byte, recLen)
	copy(payload, logBytes[start:start+recLen])
	return payload, nil
}

func (s *segment) close() error {
	if s.tiered {
		return nil
	}
	if err := s.logFile.Close(); err != nil {
		return err
	}
	return s.indexFile.Close()
}

// tierOut uploads this segment's current local bytes to coldStore under
// key, then only on confirmed success deletes the local files and marks
// the segment tiered — never the other way around (see design doc's
// "never delete before confirmed durable" invariant).
func (s *segment) tierOut(coldStore ColdStore, key string) error {
	logPath, indexPath := segmentPaths(s.dir, s.baseOffset)
	logBytes, err := os.ReadFile(logPath)
	if err != nil {
		return fmt.Errorf("read local log for tiering: %w", err)
	}
	indexBytes, err := os.ReadFile(indexPath)
	if err != nil {
		return fmt.Errorf("read local index for tiering: %w", err)
	}

	if err := coldStore.Put(key, logBytes, indexBytes); err != nil {
		return fmt.Errorf("upload to cold store: %w", err)
	}

	// upload confirmed durable — safe to close and delete local files now
	if err := s.logFile.Close(); err != nil {
		return err
	}
	if err := s.indexFile.Close(); err != nil {
		return err
	}
	if err := os.Remove(logPath); err != nil {
		return err
	}
	if err := os.Remove(indexPath); err != nil {
		return err
	}

	s.tiered = true
	s.coldKey = key
	s.coldStore = coldStore
	return nil
}

func (s *segment) remove() error {
	s.close()
	if s.tiered {
		return s.coldStore.Delete(s.coldKey)
	}
	logPath, indexPath := segmentPaths(s.dir, s.baseOffset)
	if err := os.Remove(logPath); err != nil {
		return err
	}
	return os.Remove(indexPath)
}
