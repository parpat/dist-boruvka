package distboruvka

import (
	"encoding/gob"
	"log"
	"net"
	"strconv"
)

type Message struct {
	Type string
}

type Edge struct {
	AdjNodeID int
	Weight    int    //Edge weight
	SE        string //Edge state

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

//SUBNET of docker network
const SUBNET string = "172.17.0."

//PORT
const PORT string = ":7575"

var enc *gob.Encoder

//sends message to the adjacent node of the edge
func (e *Edge) send(m Message) {
	conn, err := net.Dial("tcp", SUBNET+strconv.Itoa(e.AdjNodeID)+PORT)
	if err != nil {
		log.Println(err)
		log.Printf("conn null? %v\n", conn == nil)
	} else {
		enc = gob.NewEncoder(conn)
		err = enc.Encode(m)
		if err != nil {
			log.Fatal(err)
		}
	}
}
