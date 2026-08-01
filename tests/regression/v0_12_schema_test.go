package regression

import (
	"testing"

	"github.com/saitejasrivilli/ledgerdb/producer"
	"github.com/saitejasrivilli/ledgerdb/schema"
	"github.com/saitejasrivilli/ledgerdb/security"
)

func TestV12_CompatibleSchemaChangeAccepted(t *testing.T) {
	reg := schema.NewRegistry()

	v1 := &schema.Schema{Fields: map[string]schema.Field{
		"user_id": {Type: schema.TypeString, Required: true},
	}}
	if err := reg.Register(0, v1); err != nil {
		t.Fatalf("register v1: %v", err)
	}

	// v2 only adds an optional field — must be accepted
	v2 := &schema.Schema{Fields: map[string]schema.Field{
		"user_id": {Type: schema.TypeString, Required: true},
		"email":   {Type: schema.TypeString, Required: false},
	}}
	if err := reg.Register(0, v2); err != nil {
		t.Fatalf("expected compatible v2 registration to succeed: %v", err)
	}

	// old v1-shaped data (no "email") must still validate under v2
	if err := schema.Validate(v2, []byte(`{"user_id":"abc"}`)); err != nil {
		t.Fatalf("expected old-shaped data to validate under v2: %v", err)
	}

	// new data using the new optional field also validates
	if err := schema.Validate(v2, []byte(`{"user_id":"abc","email":"a@b.com"}`)); err != nil {
		t.Fatalf("expected new-shaped data to validate under v2: %v", err)
	}
}

func TestV12_BreakingSchemaChangeRejected(t *testing.T) {
	reg := schema.NewRegistry()

	v1 := &schema.Schema{Fields: map[string]schema.Field{
		"user_id": {Type: schema.TypeString, Required: true},
		"amount":  {Type: schema.TypeNumber, Required: true},
	}}
	if err := reg.Register(0, v1); err != nil {
		t.Fatalf("register v1: %v", err)
	}

	// v2 removes required field "amount" — must be rejected at
	// registration time, not deferred to individual writes
	v2RemovesRequired := &schema.Schema{Fields: map[string]schema.Field{
		"user_id": {Type: schema.TypeString, Required: true},
	}}
	if err := reg.Register(0, v2RemovesRequired); err == nil {
		t.Fatalf("expected registration to reject removal of required field")
	}

	// v2 narrows "amount" from number to integer — must be rejected
	v2Narrows := &schema.Schema{Fields: map[string]schema.Field{
		"user_id": {Type: schema.TypeString, Required: true},
		"amount":  {Type: schema.TypeInteger, Required: true},
	}}
	if err := reg.Register(0, v2Narrows); err == nil {
		t.Fatalf("expected registration to reject type narrowing")
	}

	// v2 adds a new required field with no default — must be rejected
	v2NewRequired := &schema.Schema{Fields: map[string]schema.Field{
		"user_id":  {Type: schema.TypeString, Required: true},
		"amount":   {Type: schema.TypeNumber, Required: true},
		"currency": {Type: schema.TypeString, Required: true},
	}}
	if err := reg.Register(0, v2NewRequired); err == nil {
		t.Fatalf("expected registration to reject new required field with no default")
	}

	// confirm v1 is still the current schema after all rejected attempts
	current, ok := reg.CurrentSchema(0)
	if !ok || current != v1 {
		t.Fatalf("expected v1 to remain the current schema after rejected changes")
	}
}

func TestV12_CheckedWriteRejectsInvalidDocumentBeforeConsensus(t *testing.T) {
	net, nodes := makeReplicatedCluster(t, 3)
	defer func() {
		for _, n := range nodes {
			n.Close()
		}
	}()
	leader := findLeader(t, net, nodes)

	reg := schema.NewRegistry()
	reg.Register(0, &schema.Schema{Fields: map[string]schema.Field{
		"user_id": {Type: schema.TypeString, Required: true},
	}})

	acl := security.NewACL()
	acl.Grant("svc", 0, security.Write)

	// missing required field — must be rejected before ever reaching Raft
	if _, err := schema.CheckedWrite(reg, acl, "svc", 0, nodes[leader], []byte(`{}`), producer.AckAll); err == nil {
		t.Fatalf("expected schema validation to reject a document missing a required field")
	}
	if nodes[leader].NextLocalOffset() != 0 {
		t.Fatalf("expected no Raft entry committed for a rejected document, got offset %d", nodes[leader].NextLocalOffset())
	}

	// valid document succeeds
	idx, err := schema.CheckedWrite(reg, acl, "svc", 0, nodes[leader], []byte(`{"user_id":"abc"}`), producer.AckAll)
	if err != nil {
		t.Fatalf("expected valid document to be accepted: %v", err)
	}
	if idx <= 0 {
		t.Fatalf("expected a positive raft index, got %d", idx)
	}
}
