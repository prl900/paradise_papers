# paradise_papers
An exercise with graph databases

## Background
For this exercise I use Dgraph as the graph database engine and backend. Dgraph is a graph database implemented in Go and designed to scale to massive volumes and workloads. This article caught my attention:
`https://blog.dgraph.io/post/benchmark-neo4j/`


## Code Structure:
`populate_dgraph.go` contains the code to extract the paradise papers data hosted in a SQL GCP instance and ingest the new graph schema on a local Dgraph instance

`shortest.go` exposes an http server the the shortest path between two given nodes specified using the id of the source node `scr_id` and the id of the destination node `dst_id`.

Example of a request:
`http://localhost:8081/?src_id=39041547&dst_id=39172370`

And its response:
`{"path":[{"id":39041547},{"id":39031075},{"id":39172370}],"parsing_ns":15000,"processing_ns":1909000,"encoding_ns":442000}`

Specifying the list of nodes traversing the graph from `src_id` to `dst_id`. The JSON struct also contains performance metrics related to the query.
