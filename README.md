# paradise_papers
An exercise with graph databases

## Background
For this exercise I use Dgraph as the graph database engine and backend. Dgraph is a graph database implemented in Go and designed to scale to massive volumes and workloads. This article caught my attention:
`https://blog.dgraph.io/post/benchmark-neo4j/`


## Code Structure:
`populate_dgraph.go` contains the code to extract the paradise papers data hosted in a SQL GCP instance and ingest the new graph schema on a local Dgraph instance

`shortest.go` exposes an http server the the shortest path between two given nodes specified using the id of the source node `scr_id` and the id of the destination node `dst_id`.

`delete_db.go` deletes the contents of the local Dgraph instance.


## How to:
* Run a local MySQL instance listening on :3306 with the paradise data loaded

* Run a local Dgraph instance using Docker
```
# Directory to store data in. This would be passed to `-v` flag.
mkdir -p /tmp/data

# Run Dgraph Zero
docker run -it -p 5080:5080 -p 6080:6080 -p 8080:8080 -p 9080:9080 -p 8000:8000 -v /tmp/data:/dgraph --name diggy dgraph/dgraph dgraph zero

# Run Dgraph Server
docker exec -it diggy dgraph server --lru_mb 2048 --zero localhost:5080

# Run Dgraph Ratel
docker exec -it diggy dgraph-ratel
```

* Install Go > 1.8 
https://golang.org/doc/install

* Install dependencies

`go get github.com/dgraph-io/dgo`
`go get github.com/go-sql-driver/mysql`

* Populate the database
```
> go build populate_database.go
> ./populate_database
```

* Run shortest path server
```
> go build shortest.go
> ./shortest
```

* On the browser

http://localhost:8081/?src_id=39041547&dst_id=39172370`

And its response:
`{"path":[{"id":39041547},{"id":39031075},{"id":39172370}],"parsing_ns":15000,"processing_ns":1909000,"encoding_ns":442000}`

Specifying the list of nodes traversing the graph from `src_id` to `dst_id`. The JSON struct also contains performance metrics related to the query.



