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

func RegisteredAddress(srcUId, dstUId string) Node {
	return Node{
		UId:               srcUId,
		RegisteredAddress: []Node{Node{UId: dstUId}},
	}
}

func OfficerOf(srcUId, dstUId string) Node {
	return Node{
		UId:       srcUId,
		OfficerOf: []Node{Node{UId: dstUId}},
	}
}

func ConnectedTo(srcUId, dstUId string) Node {
	return Node{
		UId:         srcUId,
		ConnectedTo: []Node{Node{UId: dstUId}},
	}
}

func IntermediaryOf(srcUId, dstUId string) Node {
	return Node{
		UId:            srcUId,
		IntermediaryOf: []Node{Node{UId: dstUId}},
	}
}

func SameNameAs(srcUId, dstUId string) Node {
	return Node{
		UId:        srcUId,
		SameNameAs: []Node{Node{UId: dstUId}},
	}
}

func SameIdAs(srcUId, dstUId string) Node {
	return Node{
		UId:      srcUId,
		SameIdAs: []Node{Node{UId: dstUId}},
	}
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

func main() {
	conn, err := grpc.Dial("127.0.0.1:9080", grpc.WithInsecure())
	if err != nil {
		log.Fatal("While trying to dial gRPC")
	}
	defer conn.Close()

	dc := api.NewDgraphClient(conn)
	dg := dgo.NewDgraphClient(dc)

	uid1, err := GetUId(dg, 39172370)
	fmt.Println(uid1, err)
	uid2, err := GetUId(dg, 59216527)
	fmt.Println(uid2, err)

	n := ConnectedTo(uid1, uid2)
	err = MutateNode(dg, n)
	fmt.Println(err)
}
