package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"

	"github.com/dgraph-io/dgo"
	"github.com/dgraph-io/dgo/protos/api"
	"google.golang.org/grpc"
)

var dg *dgo.Dgraph

// struct used to deserialise dgraph query response for shortest path
type PathResponse struct {
	Path []struct {
		ID int `json:"id"`
	} `json:"path"`
}

// struct used to serialise the our api response including metrics
type Response struct {
	PathResponse
	api.Latency
}

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

// get dgraph uid from a node id
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

// query dgraph to find shortest path between 2 nodes
func Shortest(dg *dgo.Dgraph, id1, id2 int) (Response, error) {
	uid1, err := GetUId(dg, id1)
	if err != nil {
		return Response{}, err
	}
	uid2, err := GetUId(dg, id2)
	if err != nil {
		return Response{}, err
	}

	q := fmt.Sprintf(`query {
	 path as shortest(from: %s, to: %s) {
	  officer_of
	  registered_address
	  connected_to
	  same_name_as
	  same_id_as
	  intermediary_of
	 }
	 path(func: uid(path)) {
	   id
	 }
	}`, uid1, uid2)

	ctx := context.Background()
	resp, err := dg.NewTxn().Query(ctx, q)
	if err != nil {
		return Response{}, err
	}

	var r PathResponse
	err = json.Unmarshal(resp.Json, &r)
	if err != nil {
		return Response{}, err
	}

	return Response{r, *resp.Latency}, nil
}

// shortest path handler for http server
func handler(w http.ResponseWriter, r *http.Request) {
	srcId := r.URL.Query().Get("src_id")
	if srcId == "" {
		w.WriteHeader(http.StatusUnprocessableEntity)
		w.Write([]byte("422 - request must specify a 'src_id' parameter"))
		return
	}

	dstId := r.URL.Query().Get("dst_id")
	if dstId == "" {
		w.WriteHeader(http.StatusUnprocessableEntity)
		w.Write([]byte("422 - request must specify a 'dst_id' parameter"))
		return
	}

	src, err := strconv.Atoi(srcId)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("400 - bad request"))
		return
	}

	dst, err := strconv.Atoi(dstId)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("400 - bad request"))
		return
	}

	path, err := Shortest(dg, src, dst)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "404 - Status not found: %v", err)
		return
	}

	enc := json.NewEncoder(w)
	enc.Encode(path)
}

func main() {
	// create connection to dgraph (pool)
	conn, err := grpc.Dial("127.0.0.1:9080", grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	dc := api.NewDgraphClient(conn)
	dg = dgo.NewDgraphClient(dc)

	// associate "handler" function with server root address
	http.HandleFunc("/", handler)
	// start the server on localhost:8081 with default mux
	log.Fatal(http.ListenAndServe(":8081", nil))
}
