package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/dgraph-io/dgo"
	"github.com/dgraph-io/dgo/protos/api"
	"google.golang.org/grpc"
)

// If omitempty is not set, then edges with empty values (0 for int/float, "" for string, false
// for bool) would be created for values not specified explicitly.
type Node struct {
	UId          string `json:"uid,omitempty"`
	Id           int    `json:"id,omitempty"`
	Labels       string `json:"labels,omitempty"`
	ValidUntil   string `json:"valid_until,omitempty"`
	CountryCodes string `json:"country_codes,omitempty"`
	Countries    string `json:"countries,omitempty"`
	SourceID     string `json:"source_id,omitempty"`
	Address      string `json:"address,omitempty"`
	Name         string `json:"name,omitempty"`
	JurisDscr    string `json:"juris_descr,omitempty"`
	ServiceProv  string `json:"service_prov,omitempty"`
	Jurisdiction string `json:"jurisdiction,omitempty"`
	ClosedDate   string `json:"closed_date,omitempty"`
	IncorpDate   string `json:"incorp_date,omitempty"`
	IBCRUC       string `json:"ibcruc,omitempty"`
	Type         string `json:"type,omitempty"`
	Status       string `json:"status,omitempty"`
	CompanyType  string `json:"company_type,omitempty"`
	Note         string `json:"note,omitempty"`
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

func main() {
	conn, err := grpc.Dial("127.0.0.1:9080", grpc.WithInsecure())
	if err != nil {
		log.Fatal("While trying to dial gRPC")
	}
	defer conn.Close()

	dc := api.NewDgraphClient(conn)
	dg := dgo.NewDgraphClient(dc)

	uid, err := GetUId(dg, 39172370)
	fmt.Println(uid, err)
	uid, err = GetUId(dg, 172370)
	fmt.Println(uid, err)
}
