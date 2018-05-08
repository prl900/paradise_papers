package main

import (
	"fmt"
	"github.com/GoogleCloudPlatform/cloudsql-proxy/proxy/dialers/mysql"
	"os"
)

type Edge struct {
	Id      int
	Node1   int
	RelType string
	Node2   int
}

type Node struct {
	Id           int
	Labels       string
	ValidUntil   string
	CountryCodes string
	Countries    string
	SourceID     string
	Address      string
	Name         string
	JurisDscr    string
	ServiceProv  string
	Jurisdiction string
	ClosedDate   string
	IncorpDate   string
	IBCRUC       string
	Type         string
	Status       string
	CompanyType  string
	Note         string
}

func main() {
	cfg := mysql.Cfg("terrascope-io:australia-southeast1:paradise", "root", os.Getenv("MySQL_PSSWD"))
	cfg.DBName = "paradise"
	db, err := mysql.DialCfg(cfg)
	if err != nil {
		return
	}

	rows, err := db.Query("SELECT * FROM edges")
	if err != nil {
		return
	}

	for rows.Next() {
		var e Edge
		err = rows.Scan(&e.Id, &e.Node1, &e.RelType, &e.Node2)
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Println(e)
		break
	}

	for _, nodeName := range []string{"address", "entity", "intermediary", "officer", "other"} {
		rows, err = db.Query(fmt.Sprintf("SELECT * FROM `nodes.%s`", nodeName))
		if err != nil {
			return
		}

		fmt.Println(nodeName)

		i := 0
		for rows.Next() {
			var n Node
			err = rows.Scan(&n.Labels, &n.ValidUntil, &n.CountryCodes, &n.Countries, &n.Id,
				&n.SourceID, &n.Address, &n.Name, &n.JurisDscr, &n.ServiceProv, &n.Jurisdiction,
				&n.ClosedDate, &n.IncorpDate, &n.IBCRUC, &n.Type, &n.Status, &n.CompanyType, &n.Note)

			if err != nil {
				fmt.Println(err)
				return
			}
			fmt.Println(n)
			if i > 30 {
				break
			}
			i++
		}

	}

}
