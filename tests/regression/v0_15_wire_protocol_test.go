package regression

import (
	"context"
	"testing"
	"time"

	"github.com/saitejasrivilli/ledgerdb/mongowire"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// TestWireProtocol_InsertFindUpdateDelete drives the full command cycle
// using the REAL official MongoDB Go driver against a locally running
// mongowire.Server — not a hand-rolled test client. If the real driver's
// calls succeed and return correct data, the wire protocol
// implementation is correct by the same measure a real application would
// judge it. See docs/design_wire_protocol.md.
func TestWireProtocol_InsertFindUpdateDelete(t *testing.T) {
	net, nodes := makeDocStoreCluster(t, 3, "")
	defer func() {
		for _, n := range nodes {
			n.Close()
		}
	}()
	leader := findDocStoreLeader(t, net, nodes)

	srv, err := mongowire.Listen("127.0.0.1:0", nodes[leader])
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(options.Client().
		ApplyURI("mongodb://" + srv.Addr()).
		SetDirect(true).
		SetServerSelectionTimeout(3 * time.Second))
	if err != nil {
		t.Fatalf("mongo.Connect: %v", err)
	}
	defer client.Disconnect(ctx)

	coll := client.Database("ledgerdb").Collection("items")

	// insert
	res, err := coll.InsertOne(ctx, bson.M{"_id": "item-1", "name": "widget", "qty": int32(10)})
	if err != nil {
		t.Fatalf("InsertOne: %v", err)
	}
	if res.InsertedID != "item-1" {
		t.Fatalf("InsertedID = %v, want item-1", res.InsertedID)
	}

	// find by _id
	var got bson.M
	if err := coll.FindOne(ctx, bson.M{"_id": "item-1"}).Decode(&got); err != nil {
		t.Fatalf("FindOne: %v", err)
	}
	if got["name"] != "widget" {
		t.Fatalf("found doc name = %v, want widget", got["name"])
	}

	// insert a second document, then find-all (empty filter)
	if _, err := coll.InsertOne(ctx, bson.M{"_id": "item-2", "name": "gadget", "qty": int32(5)}); err != nil {
		t.Fatalf("InsertOne item-2: %v", err)
	}
	cursor, err := coll.Find(ctx, bson.M{})
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	var all []bson.M
	if err := cursor.All(ctx, &all); err != nil {
		t.Fatalf("cursor.All: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("expected 2 documents from full scan, got %d", len(all))
	}

	// update via $set
	updateRes, err := coll.UpdateOne(ctx, bson.M{"_id": "item-1"}, bson.M{"$set": bson.M{"qty": int32(99)}})
	if err != nil {
		t.Fatalf("UpdateOne: %v", err)
	}
	if updateRes.MatchedCount != 1 {
		t.Fatalf("MatchedCount = %d, want 1", updateRes.MatchedCount)
	}
	var updated bson.M
	if err := coll.FindOne(ctx, bson.M{"_id": "item-1"}).Decode(&updated); err != nil {
		t.Fatalf("FindOne after update: %v", err)
	}
	if qty, ok := updated["qty"].(int32); !ok || qty != 99 {
		t.Fatalf("qty after update = %v, want 99", updated["qty"])
	}

	// delete
	delRes, err := coll.DeleteOne(ctx, bson.M{"_id": "item-2"})
	if err != nil {
		t.Fatalf("DeleteOne: %v", err)
	}
	if delRes.DeletedCount != 1 {
		t.Fatalf("DeletedCount = %d, want 1", delRes.DeletedCount)
	}
	if err := coll.FindOne(ctx, bson.M{"_id": "item-2"}).Err(); err != mongo.ErrNoDocuments {
		t.Fatalf("expected item-2 to be gone, got err=%v", err)
	}
}
