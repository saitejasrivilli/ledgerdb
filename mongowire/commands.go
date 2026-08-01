package mongowire

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/saitejasrivilli/ledgerdb/docstore"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// toM normalizes a decoded BSON value into bson.M. The driver decodes
// TOP-level documents into whatever type Unmarshal's target is (bson.M,
// here) but NESTED sub-documents inside an interface{}-typed field
// always come back as bson.D (ordered), never bson.M — so every nested
// document this package reaches into (a filter's value, an update's "u"
// document, "$set", etc.) needs this conversion before a `.(bson.M)`
// type assertion would otherwise silently fail and return nil.
func toM(v interface{}) bson.M {
	switch t := v.(type) {
	case bson.M:
		return t
	case bson.D:
		m := bson.M{}
		for _, e := range t {
			m[e.Key] = e.Value
		}
		return m
	default:
		return nil
	}
}

func newID() string {
	b := make([]byte, 12)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// docID extracts _id from a decoded document, generating one if absent
// — mirrors real MongoDB's behavior of assigning an ObjectID client- or
// server-side when a document is inserted without one.
func docID(doc bson.M) string {
	if v, ok := doc["_id"]; ok {
		return fmt.Sprintf("%v", v)
	}
	return newID()
}

func (s *Server) handleInsert(cmd bson.M) bson.M {
	docsRaw, _ := cmd["documents"].([]interface{})
	var muts []docstore.Mutation
	for _, d := range docsRaw {
		doc, ok := d.(bson.M)
		if !ok {
			continue
		}
		id := docID(doc)
		doc["_id"] = id
		data, err := docToJSON(doc)
		if err != nil {
			return bson.M{"ok": 0.0, "errmsg": err.Error()}
		}
		muts = append(muts, docstore.Mutation{Op: "put", ID: id, Data: data})
	}
	if len(muts) == 0 {
		return bson.M{"ok": 1.0, "n": int32(0)}
	}
	if err := s.propose(muts); err != nil {
		return bson.M{"ok": 0.0, "errmsg": err.Error()}
	}
	return bson.M{"ok": 1.0, "n": int32(len(muts))}
}

// filterID extracts an _id-equality filter — the only filter shape this
// version's find/update/delete support (see design doc's scope boundary).
func filterID(filter bson.M) (id string, isIDFilter bool) {
	if len(filter) == 0 {
		return "", false
	}
	if v, ok := filter["_id"]; ok {
		return fmt.Sprintf("%v", v), true
	}
	return "", false
}

func (s *Server) handleFind(cmd bson.M) bson.M {
	filter := toM(cmd["filter"])
	snap := s.store.Snapshot()

	var ids []string
	if id, ok := filterID(filter); ok {
		ids = []string{id}
	} else {
		ids = snap.Scan()
	}

	batch := []bson.M{} // never nil: BSON encodes a nil slice as null, but drivers require an array here even when empty
	for _, id := range ids {
		doc, ok := snap.Get(id)
		if !ok {
			continue
		}
		var m bson.M
		blob, err := json.Marshal(doc)
		if err != nil {
			continue
		}
		if err := bson.UnmarshalExtJSON(blob, true, &m); err != nil {
			continue
		}
		m["_id"] = id
		batch = append(batch, m)
	}

	collName, _ := cmd["find"].(string)
	dbName, _ := cmd["$db"].(string)
	return bson.M{
		"ok": 1.0,
		"cursor": bson.M{
			"firstBatch": batch,
			"id":         int64(0),
			"ns":         dbName + "." + collName,
		},
	}
}

func (s *Server) handleUpdate(cmd bson.M) bson.M {
	updatesRaw, _ := cmd["updates"].([]interface{})
	snap := s.store.Snapshot()

	var matched, modified int32
	var muts []docstore.Mutation
	for _, u := range updatesRaw {
		upd, ok := u.(bson.M)
		if !ok {
			continue
		}
		q := toM(upd["q"])
		id, ok := filterID(q)
		if !ok {
			continue
		}
		doc, exists := snap.Get(id)
		if !exists {
			continue
		}
		matched++

		uDoc := toM(upd["u"])
		set := toM(uDoc["$set"])
		merged := docstore.Document{}
		for k, v := range doc {
			merged[k] = v
		}
		for k, v := range set {
			merged[k] = v
		}
		merged["_id"] = id

		data, err := docToJSON(merged)
		if err != nil {
			return bson.M{"ok": 0.0, "errmsg": err.Error()}
		}
		muts = append(muts, docstore.Mutation{Op: "put", ID: id, Data: data})
		modified++
	}

	if len(muts) > 0 {
		if err := s.propose(muts); err != nil {
			return bson.M{"ok": 0.0, "errmsg": err.Error()}
		}
	}
	return bson.M{"ok": 1.0, "n": matched, "nModified": modified}
}

func (s *Server) handleDelete(cmd bson.M) bson.M {
	deletesRaw, _ := cmd["deletes"].([]interface{})
	var muts []docstore.Mutation
	for _, d := range deletesRaw {
		del, ok := d.(bson.M)
		if !ok {
			continue
		}
		q := toM(del["q"])
		id, ok := filterID(q)
		if !ok {
			continue
		}
		muts = append(muts, docstore.Mutation{Op: "delete", ID: id})
	}
	if len(muts) == 0 {
		return bson.M{"ok": 1.0, "n": int32(0)}
	}
	if err := s.propose(muts); err != nil {
		return bson.M{"ok": 0.0, "errmsg": err.Error()}
	}
	return bson.M{"ok": 1.0, "n": int32(len(muts))}
}
