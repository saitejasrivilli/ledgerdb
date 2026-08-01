package regression

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/saitejasrivilli/ledgerdb/api"
)

// TestRESTAPI_CRUDRoundTrip drives PUT/GET/DELETE/query through real
// net/http requests against a real listening HTTP server
// (httptest.NewServer, not the in-process httptest.NewRecorder
// shortcut), backed by a real 3-node replicated cluster. See
// docs/design_rest_api.md.
func TestRESTAPI_CRUDRoundTrip(t *testing.T) {
	net, nodes := makeDocStoreCluster(t, 3, "category")
	defer func() {
		for _, n := range nodes {
			n.Close()
		}
	}()
	leader := findDocStoreLeader(t, net, nodes)

	srv := httptest.NewServer(api.New(nodes[leader]))
	defer srv.Close()

	client := srv.Client()

	// PUT
	body := bytes.NewBufferString(`{"name":"widget","category":"tools"}`)
	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/docs/item-1", body)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status = %d, want 200", resp.StatusCode)
	}
	resp.Body.Close()

	// GET
	resp, err = client.Get(srv.URL + "/docs/item-1")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET status = %d, want 200", resp.StatusCode)
	}
	var doc map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		t.Fatalf("decode GET body: %v", err)
	}
	if doc["name"] != "widget" {
		t.Fatalf("GET body name = %v, want widget", doc["name"])
	}

	// GET missing document -> 404
	resp, err = client.Get(srv.URL + "/docs/does-not-exist")
	if err != nil {
		t.Fatalf("GET missing: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET missing status = %d, want 404", resp.StatusCode)
	}

	// query by indexed field
	body2 := bytes.NewBufferString(`{"name":"gadget","category":"tools"}`)
	req2, _ := http.NewRequest(http.MethodPut, srv.URL+"/docs/item-2", body2)
	resp2, err := client.Do(req2)
	if err != nil {
		t.Fatalf("PUT item-2: %v", err)
	}
	resp2.Body.Close()

	resp, err = client.Get(srv.URL + "/docs?category=tools")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer resp.Body.Close()
	var docs []map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&docs); err != nil {
		t.Fatalf("decode query body: %v", err)
	}
	if len(docs) != 2 {
		t.Fatalf("query returned %d docs, want 2", len(docs))
	}

	// DELETE
	req3, _ := http.NewRequest(http.MethodDelete, srv.URL+"/docs/item-1", nil)
	resp, err = client.Do(req3)
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE status = %d, want 204", resp.StatusCode)
	}

	resp, err = client.Get(srv.URL + "/docs/item-1")
	if err != nil {
		t.Fatalf("GET after delete: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET after delete status = %d, want 404", resp.StatusCode)
	}
}
