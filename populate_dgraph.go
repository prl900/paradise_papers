package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"

	"github.com/dgraph-io/dgo"
	"github.com/dgraph-io/dgo/protos/api"
	_ "github.com/go-sql-driver/mysql"
	"google.golang.org/grpc"
)

// If omitempty is not set, then edges with empty values (0 for int/float, "" for string, false
// for bool) would be created for values not specified explicitly.
type Node struct {
	UId               string `json:"uid,omitempty"`
	Id                int    `json:"id,omitempty"`
	Labels            string `json:"labels,omitempty"`
	ValidUntil        string `json:"valid_until,omitempty"`
	CountryCodes      string `json:"country_codes,omitempty"`
	Countries         string `json:"countries,omitempty"`
	SourceID          string `json:"source_id,omitempty"`
	Address           string `json:"address,omitempty"`
	Name              string `json:"name,omitempty"`
	JurisDscr         string `json:"juris_descr,omitempty"`
	ServiceProv       string `json:"service_prov,omitempty"`
	Jurisdiction      string `json:"jurisdiction,omitempty"`
	ClosedDate        string `json:"closed_date,omitempty"`
	IncorpDate        string `json:"incorp_date,omitempty"`
	IBCRUC            string `json:"ibcruc,omitempty"`
	Type              string `json:"type,omitempty"`
	Status            string `json:"status,omitempty"`
	CompanyType       string `json:"company_type,omitempty"`
	Note              string `json:"note,omitempty"`
	RegisteredAddress []Node `json:"registered_address,omitempty"`
	OfficerOf         []Node `json:"officer_of,omitempty"`
	ConnectedTo       []Node `json:"connected_to,omitempty"`
	IntermediaryOf    []Node `json:"imtermediary_of,omitempty"`
	SameNameAs        []Node `json:"same_name_as,omitempty"`
	SameIdAs          []Node `json:"same_id_as,omitempty"`
}

type Edge struct {
	Id      int    `json:"id,omitempty"`
	Node1   int    `json:"node1,omitempty"`
	RelType string `json:"rel_type,omitempty"`
	Node2   int    `json:"node2,omitempty"`
}

// Gets the internal dgrap uid from an id
func GetUId(dg *dgo.Dgraph, nodeId int) (string, error) {
	q := fmt.Sprintf(`query Me($id: int){
		me(func: eq(id, %d)) {
			address
			uid
		}
	}`, nodeId)

	ctx := context.Background()
	resp, err := dg.NewTxn().Query(ctx, q)
	if err != nil {
		return "", err
	}

	type Root struct {
		Me []Node `json:"me"`
	}

	var r Root
	err = json.Unmarshal(resp.Json, &r)
	if err != nil {
		return "", err
	}

	if len(r.Me) != 1 {
		return "", fmt.Errorf("node_id %d is not in database", nodeId)
	}

	return r.Me[0].UId, nil
}

// Returns a new Node with the specified relation
func RelationNode(rel, srcUId, dstUId string) Node {
	switch rel {
	case "registered_address":
		return Node{
			UId:               srcUId,
			RegisteredAddress: []Node{Node{UId: dstUId}},
		}
	case "officer_of":
		return Node{
			UId:       srcUId,
			OfficerOf: []Node{Node{UId: dstUId}},
		}
	case "connected_to":
		return Node{
			UId:         srcUId,
			ConnectedTo: []Node{Node{UId: dstUId}},
		}
	case "intermediary_of":
		return Node{
			UId:            srcUId,
			IntermediaryOf: []Node{Node{UId: dstUId}},
		}
	case "same_name_as":
		return Node{
			UId:        srcUId,
			SameNameAs: []Node{Node{UId: dstUId}},
		}
	case "same_id_as":
		return Node{
			UId:      srcUId,
			SameIdAs: []Node{Node{UId: dstUId}},
		}
	}

	return Node{}
}

// commits a node mutation on dgraph
func MutateNode(dg *dgo.Dgraph, n Node) error {

	mu := &api.Mutation{
		CommitNow: true,
	}
	pb, err := json.Marshal(n)
	if err != nil {
		return err
	}

	mu.SetJson = pb
	ctx := context.Background()
	_, err = dg.NewTxn().Mutate(ctx, mu)

	return err
}

// ingests a paradise nodes.* table into dgraph
func IngestNodeTable(dg *dgo.Dgraph, db *sql.DB, tName string) error {
	rows, err := db.Query(fmt.Sprintf("SELECT * FROM `%s`", tName))
	if err != nil {
		return err
	}

	for rows.Next() {
		var n Node
		err = rows.Scan(&n.Labels, &n.ValidUntil, &n.CountryCodes, &n.Countries, &n.Id,
			&n.SourceID, &n.Address, &n.Name, &n.JurisDscr, &n.ServiceProv, &n.Jurisdiction,
			&n.ClosedDate, &n.IncorpDate, &n.IBCRUC, &n.Type, &n.Status, &n.CompanyType, &n.Note)
		if err != nil {
			return err
		}

		err = MutateNode(dg, n)
		if err != nil {
			return err
		}
	}

	return nil
}

// ingests paradise edges table as relations between existing dgraph nodes
func DefineEdges(dg *dgo.Dgraph, db *sql.DB) error {
	rows, err := db.Query("SELECT * FROM edges")
	if err != nil {
		return err
	}

	for rows.Next() {
		var e Edge
		err = rows.Scan(&e.Id, &e.Node1, &e.RelType, &e.Node2)
		if err != nil {
			return err
		}

		uid1, err := GetUId(dg, e.Node1)
		if err != nil {
			return err
		}
		uid2, err := GetUId(dg, e.Node2)
		if err != nil {
			return err
		}

		n := RelationNode(e.RelType, uid1, uid2)
		err = MutateNode(dg, n)
		if err != nil {
			return err
		}
	}

	return nil
}

func main() {
	// connection to mysql
	db, err := sql.Open("mysql", "root:admin@tcp(127.0.0.1:3306)/paradise?charset=utf8")
	if err != nil {
		log.Fatal(err)
	}

	// connection to dgraph
	conn, err := grpc.Dial("127.0.0.1:9080", grpc.WithInsecure())
	if err != nil {
		log.Fatal("While trying to dial gRPC")
	}
	defer conn.Close()

	dc := api.NewDgraphClient(conn)
	dg := dgo.NewDgraphClient(dc)

	ctx := context.Background()
	// setting dgrpah schema
	op := &api.Operation{}
	op.Schema = `
		id: int @index(int) .
		address: string .
	`
	err = dg.Alter(ctx, op)
	if err != nil {
		log.Fatal(err)
	}

	// loop though nodes.* tables for ingestion
	for _, tName := range []string{"nodes.address", "nodes.entity", "nodes.intermediary", "nodes.officer", "nodes.other"} {
		err = IngestNodeTable(dg, db, tName)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(tName, "done")
	}

	// ingest edges between the previously defined nodes
	DefineEdges(dg, db)

}
