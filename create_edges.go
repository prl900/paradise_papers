package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/GoogleCloudPlatform/cloudsql-proxy/proxy/dialers/mysql"

	"github.com/dgraph-io/dgo"
	"github.com/dgraph-io/dgo/protos/api"
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
	conn, err := grpc.Dial("127.0.0.1:9080", grpc.WithInsecure())
	if err != nil {
		log.Fatal("While trying to dial gRPC")
	}
	defer conn.Close()

	dc := api.NewDgraphClient(conn)
	dg := dgo.NewDgraphClient(dc)

	cfg := mysql.Cfg("terrascope-io:australia-southeast1:paradise", "root", os.Getenv("MySQL_PSSWD"))
	cfg.DBName = "paradise"
	db, err := mysql.DialCfg(cfg)
	if err != nil {
		log.Fatal(err)
	}

	DefineEdges(dg, db)
}
