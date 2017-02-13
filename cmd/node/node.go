package main

import (
	"fmt"
	"log"
	"strings"

	distb "github.com/parpat/distboruvka"
)

const GATEWAY string = "1"

func processMessages(reqs chan distb.Message) {
	for m := range reqs {
		fmt.Println("request received")
		if m.Type == "ReqAdjEdges" {
			sendEdges()
		}
	}
}

func sendEdges() {
	msg := distb.Message{Edges: *ThisNode.AdjacencyList}
	distb.SendMessage(msg, GATEWAY)
}

var (
	ThisNode distb.Node
	requests chan distb.Message
	Logger   *log.Logger
)

func init() {
	hostName, hostIP := distb.GetHostInfo()
	octets := strings.Split(hostIP, ".")
	fmt.Printf("My ID is: %s\n", octets[3])
	nodeID := octets[3]
	edges := distb.GetEdgesFromFile("boruvka.conf", nodeID)

	ThisNode = distb.Node{
		ID:            nodeID,
		Name:          hostName,
		AdjacencyList: &edges}

	//logfile, err := os.Create("/logs/log" + strconv.Itoa(nodeID))
	// if err != nil {
	// 	log.Fatal(err)
	// }
	//Logger = log.New(logfile, "logger: ", log.Lshortfile)
	_ = Logger

}

func main() {
	requests = make(chan distb.Message, 5)
	notListening := make(chan bool)

	go distb.ListenAndServeTCP(notListening, requests)
	//Process incomming messages
	go processMessages(requests)

	go distb.SetNodeInfo(ThisNode.Name, ThisNode.ID)

	<-notListening
}
