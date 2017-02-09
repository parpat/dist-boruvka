package main

import (
	"fmt"
	"log"
	"strconv"
	"strings"

	distb "github.com/parpat/distboruvka"
)

const GATEWAY string = "1"

//Node is the current instance
type Node struct {
	ID            int
	adjacencyList *distb.Edges
}

func processMessage(reqs chan distb.Message) {
	for m := range reqs {
		fmt.Println("request received")
		if m.Type == "ReqAdjEdges" {
			sendEdges()
		}
	}
}

func sendEdges() {
	msg := distb.Message{Edges: *ThisNode.adjacencyList}
	distb.SendMessage(msg, GATEWAY)
}

var (
	HostName string
	HostIP   string
	ThisNode Node
	requests chan distb.Message
	Logger   *log.Logger
)

func init() {
	HostName, HostIP = distb.GetHostInfo()
	octets := strings.Split(HostIP, ".")
	fmt.Printf("My ID is: %s\n", octets[3])
	nodeID, err := strconv.Atoi(octets[3])
	edges, _ := distb.GetEdgesFromFile("boruvka.conf", nodeID)
	if err != nil {
		log.Fatal(err)
	}

	ThisNode = Node{
		ID:            nodeID,
		adjacencyList: &edges}

	//logfile, err := os.Create("/logs/log" + strconv.Itoa(nodeID))
	if err != nil {
		log.Fatal(err)
	}
	//Logger = log.New(logfile, "logger: ", log.Lshortfile)
	_ = Logger

}

func main() {
	requests = make(chan distb.Message, 5)
	notListening := make(chan bool)

	go distb.ListenAndServeTCP(notListening, requests)
	//Process incomming messages
	go processMessage(requests)

	<-notListening
}
