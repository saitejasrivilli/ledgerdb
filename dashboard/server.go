// Package dashboard implements the small status UI described in
// docs/design_dashboard_ui.md — a /status JSON endpoint plus a static
// HTML/JS page that polls it. Explicitly the lowest-value of the three
// v0.15-v0.17 additions for interview purposes: a demo artifact, not new
// correctness surface, scoped accordingly (no build tooling, no
// framework).
package dashboard

import (
	_ "embed"
	"encoding/json"
	"net/http"
)

// NodeStater is the one thing the dashboard needs from each cluster node.
type NodeStater interface {
	GetState() (term int, isLeader bool)
}

type NodeStatus struct {
	Index    int  `json:"index"`
	Term     int  `json:"term"`
	IsLeader bool `json:"is_leader"`
}

type ClusterStatus struct {
	Nodes []NodeStatus `json:"nodes"`
}

//go:embed index.html
var indexHTML []byte

// Server serves the dashboard's static page and its /status JSON
// endpoint, reading real state from nodes on every request — not cached,
// not placeholder.
type Server struct {
	nodes []NodeStater
	mux   *http.ServeMux
}

func New(nodes ...NodeStater) *Server {
	s := &Server{nodes: nodes, mux: http.NewServeMux()}
	s.mux.HandleFunc("/status", s.handleStatus)
	s.mux.HandleFunc("/", s.handleIndex)
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(indexHTML)
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	status := ClusterStatus{}
	for i, n := range s.nodes {
		term, isLeader := n.GetState()
		status.Nodes = append(status.Nodes, NodeStatus{Index: i, Term: term, IsLeader: isLeader})
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}
