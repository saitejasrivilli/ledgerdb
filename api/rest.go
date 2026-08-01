// Package api implements the REST layer described in
// docs/design_rest_api.md — a thin HTTP translation over the existing
// docstore (v1.0), same composition pattern as mongowire (v0.15) applied
// to a different wire format.
package api

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/saitejasrivilli/ledgerdb/docstore"
)

// Store is the subset of docstore.ReplicatedDocStore this API needs.
type Store interface {
	Propose(muts []docstore.Mutation) (raftIndex int, isLeader bool, err error)
	WaitApplied(raftIndex int, timeout time.Duration) bool
	Snapshot() *docstore.Snapshot
}

type Server struct {
	store Store
	mux   *http.ServeMux
}

func New(store Store) *Server {
	s := &Server{store: store, mux: http.NewServeMux()}
	s.mux.HandleFunc("/docs/", s.handleDoc)
	s.mux.HandleFunc("/docs", s.handleDocs)
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

// handleDoc serves /docs/{id} — PUT (upsert), GET, DELETE.
func (s *Server) handleDoc(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/docs/")
	if id == "" {
		http.Error(w, "missing document id", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodPut:
		s.putDoc(w, r, id)
	case http.MethodGet:
		s.getDoc(w, id)
	case http.MethodDelete:
		s.deleteDoc(w, id)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) putDoc(w http.ResponseWriter, r *http.Request, id string) {
	var body map[string]interface{}
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&body); err != nil {
		http.Error(w, "malformed JSON body: "+err.Error(), http.StatusBadRequest)
		return
	}
	body["_id"] = id
	data, err := json.Marshal(body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := s.propose([]docstore.Mutation{{Op: "put", ID: id, Data: data}}); err != nil {
		writeProposeError(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) getDoc(w http.ResponseWriter, id string) {
	snap := s.store.Snapshot()
	doc, ok := snap.Get(id)
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, doc)
}

func (s *Server) deleteDoc(w http.ResponseWriter, id string) {
	snap := s.store.Snapshot()
	if _, ok := snap.Get(id); !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if err := s.propose([]docstore.Mutation{{Op: "delete", ID: id}}); err != nil {
		writeProposeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleDocs serves /docs — full scan, or ?field=value query-by-index.
func (s *Server) handleDocs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	snap := s.store.Snapshot()

	var ids []string
	if q := r.URL.Query(); len(q) > 0 {
		for _, values := range q {
			if len(values) > 0 {
				ids = snap.QueryByIndex(values[0])
				break
			}
		}
	} else {
		ids = snap.Scan()
	}

	docs := []docstore.Document{}
	for _, id := range ids {
		if doc, ok := snap.Get(id); ok {
			docs = append(docs, doc)
		}
	}
	writeJSON(w, http.StatusOK, docs)
}

func (s *Server) propose(muts []docstore.Mutation) error {
	idx, isLeader, err := s.store.Propose(muts)
	if err != nil {
		return &proposeErr{status: http.StatusInternalServerError, msg: err.Error()}
	}
	if !isLeader {
		return &proposeErr{status: http.StatusServiceUnavailable, msg: "not leader, retry against another node"}
	}
	if !s.store.WaitApplied(idx, 2*time.Second) {
		return &proposeErr{status: http.StatusGatewayTimeout, msg: "write proposed but did not commit within timeout"}
	}
	return nil
}

type proposeErr struct {
	status int
	msg    string
}

func (e *proposeErr) Error() string { return e.msg }

func writeProposeError(w http.ResponseWriter, err error) {
	if pe, ok := err.(*proposeErr); ok {
		http.Error(w, pe.msg, pe.status)
		return
	}
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
