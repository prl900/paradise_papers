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

func IngestNodeTable(dg *dgo.Dgraph, db *sql.DB, tName string) error {
	rows, err := db.Query(fmt.Sprintf("SELECT * FROM `%s`", tName))
	if err != nil {
		return err
	}

	ctx := context.Background()
	for rows.Next() {
		var n Node
		err = rows.Scan(&n.Labels, &n.ValidUntil, &n.CountryCodes, &n.Countries, &n.Id,
			&n.SourceID, &n.Address, &n.Name, &n.JurisDscr, &n.ServiceProv, &n.Jurisdiction,
			&n.ClosedDate, &n.IncorpDate, &n.IBCRUC, &n.Type, &n.Status, &n.CompanyType, &n.Note)
		if err != nil {
			return err
		}

		mu := &api.Mutation{
			CommitNow: true,
		}
		pb, err := json.Marshal(n)
		if err != nil {
			return err
		}

		mu.SetJson = pb
		_, err = dg.NewTxn().Mutate(ctx, mu)
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

	ctx := context.Background()
	op := &api.Operation{}
	op.Schema = `
		id: int @index(int) .
		address: string .
	`
	err = dg.Alter(ctx, op)
	if err != nil {
		log.Fatal(err)
	}

	for _, tName := range []string{"nodes.address", "nodes.entity", "nodes.intermediary", "nodes.officer", "nodes.other"} {
		err = IngestNodeTable(dg, db, tName)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(tName, "done")
	}
}
