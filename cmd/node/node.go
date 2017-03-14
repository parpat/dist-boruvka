package main

import (
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"

	"strconv"

	"math"

	distb "github.com/parpat/distboruvka"
)

//GATEWAY is the last octet of the docker subnetwork
const GATEWAY string = "1"

func processMessages(reqs chan distb.Message) {
	for m := range reqs {
		fmt.Println("request received from: ", m.SourceID)
		if m.Type == "ReqAdjEdges" {
			sendEdges()
		}
		if m.Type == "MSTBranch" {
			markBranch(m.Edges[0])
		}
		if m.Type == "PushSum" {
			pushSum(m.S, m.W)
		}
	}

}

func sendEdges() {
	msg := distb.Message{Edges: *ThisNode.AdjacencyList}
	distb.SendMessage(msg, GATEWAY)
}

func markBranch(e distb.Edge) {
	idx := ThisNode.FindEdgeIdx(e.Weight)
	(*ThisNode.AdjacencyList)[idx].SE = "Branch"
	fmt.Printf("%v is now a Branch\n", e.Weight)
}

func pushSum(st, wt float64) {
	S += st
	W += wt

	//Choose random neighbor
	rand.Seed(time.Now().UnixNano())
	randNeighbor := (*ThisNode.AdjacencyList)[rand.Intn(len(*ThisNode.AdjacencyList))]

	//Send pair (0.5S, 0.5W)
	sh := S / 2
	wh := W / 2
	msgPush := distb.Message{Type: "PushSum", S: sh, W: wh, SourceID: ThisNode.ID}
	time.Sleep(time.Millisecond * 50)
	fmt.Println("Sent to: ", randNeighbor.AdjNodeID)
	randNeighbor.Send(msgPush)

	S += sh
	W += wh

	avg := S / W
	if math.IsInf(avg, 0) {
		//fmt.Println("Inf S: ", S, " W: ", W)
		log.Fatalln("Inf S: ", S, " W: ", W)
	} else if math.IsNaN(avg) {
		fmt.Println("NaN S: ", S, " W: ", W)
	} else {
		fmt.Println("Current Average: ", S/W)
	}

}

var (
	//ThisNode local attributes of the node
	ThisNode  distb.Node
	requests  chan distb.Message
	Logger    *log.Logger
	S         float64
	W         float64
	startpush bool
)

func init() {
	hostName, hostIP := distb.GetHostInfo()
	octets := strings.Split(hostIP, ".")
	fmt.Printf("My ID is: %s\n", octets[3])
	nodeID := octets[3]
	edges, pushSumStart := distb.GetEdgesFromFile("boruvka.conf", nodeID)

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

	//Initializing sum var
	S, _ = strconv.ParseFloat(nodeID, 64)
	W = 1

	//Checking if current process will begin pushSum
	if pushSumStart == nodeID {
		startpush = true
	}
}

func main() {
	requests = make(chan distb.Message, 5)
	notListening := make(chan bool)

	go distb.ListenAndServeTCP(notListening, requests)
	//Process incomming messages
	go processMessages(requests)

	go distb.SetNodeInfo(ThisNode.Name, ThisNode.ID)

	if startpush {
		time.Sleep(10 * time.Second)
		pushSum(0, 0)
	}
	<-notListening
}
