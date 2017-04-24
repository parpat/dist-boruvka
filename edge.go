package distboruvka

import (
	"encoding/gob"
	"log"
	"net"
	"sync"
)

//Edge is the overlay link between nodes
type Edge struct {
	AdjNodeID    string
	Weight       int    //Edge weight
	SE           string //Edge state
	Origin       string
	MessageCount int
}

//Edge States
const (
	Basic string = "Basic" //not yet decided whether the edge is part
	//of the MST or not
	Branch   string = "Branch"   //The edge is part of the MST
	Rejected string = "Rejected" //The edge is NOT part of the MST

)

//Edges is a sortable edgelist
type Edges []Edge

func (e Edges) Len() int           { return len(e) }
func (e Edges) Swap(i, j int)      { e[i], e[j] = e[j], e[i] }
func (e Edges) Less(i, j int) bool { return e[i].Weight < e[j].Weight }

//Reusing this encoder variable creates a race condition when multiple
//routines invoke *Edge.Send() and write assign NewEncoder writes
//var enc *gob.Encoder

//Send message to the adjacent node of the edge
func (e *Edge) Send(m *Message) {
	conn, err := net.Dial("tcp", SUBNET+e.AdjNodeID+":"+PORT)
	if err != nil {
		log.Println(err)
		log.Printf("conn null? %v\n", conn == nil)
	} else {
		m.SourceID = e.Origin
		enc := gob.NewEncoder(conn)
		err = enc.Encode(m)
		if err != nil {
			log.Fatal(err)
		}
	}
}

//Node is the container's info
type Node struct {
	ID            string
	Name          string
	AdjacencyList *Edges
	sync.RWMutex
	AdjacencyMap map[string]*Edge //AdacencyMap holds edge for each adjacent node
}

//FindEdgeIdx returns the index in the adjacency list
func (n *Node) FindEdgeIdx(weight int) int {
	for i, e := range *n.AdjacencyList {
		if e.Weight == weight {
			return i
		}
	}
	return 0
}
