package main

import (
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"math"

	distb "github.com/parpat/distboruvka"
)

//GATEWAY is the last octet of the docker subnetwork
const GATEWAY string = "1"

func processMessages(reqs chan distb.Message) {
	for m := range reqs {
		fmt.Println("request received from: ", m.SourceID)
		switch m.Type {
		case "ReqAdjEdges":
			sendEdges()

		case "MSTBranch":
			markBranch(m.Edges[0])

		case "PushSum":
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

//pushSum protocol is a form of network aggregation used to calculate
//the average value between nodes in a distributed fashion. The propagation
//of data is achieved using simple gossip protocol by selecting a uniformly random neighbor
//to send data to.
var lastRatio = 0.0
var ratioConvergeCount = 0
var converged = false
var convergenceDiff = 0.00000001

func pushSum(st, wt float64) {
	if !PushSumActive {
		//In case first node to start push sum then hold barrier
		if err := distb.Barrier.Hold(); err != nil {
			log.Printf("could not hold barrier (%v)\n", err)
		}
		go watchBarrier()

		lastRatio = 0.0
		ratioConvergeCount = 0
		converged = false

		S = getCurrMaxTraffic()
		W = 1
		rwmutex.Lock()
		PushSumActive = true
		rwmutex.Unlock()
	}
	S += st
	W += wt
	ratio := S / W
	//Stop disseminating messages once converged
	//Converged when ratio does not change more than 10^-8 for 3 consecutive values
	diff := ratio - lastRatio
	lastRatio = ratio

	if math.Abs(diff) < convergenceDiff {
		ratioConvergeCount++
		if ratioConvergeCount >= 3 {
			converged = true
			if err := distb.Barrier.Release(); err != nil {
				log.Printf("could not Release barrier (%v)\n", err)
			} else {
				log.Println("Barrier Released")
			}
			fmt.Println("CONVERGED PUSH-SUM")
			return
		}
	} else {
		ratioConvergeCount = 0
	}

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

	//if math.IsInf(avg, 0) {
	//fmt.Println("Inf S: ", S, " W: ", W)
	//	log.Fatalln("Inf S: ", S, " W: ", W)
	//} else if math.IsNaN(avg) {
	//fmt.Println("NaN S: ", S, " W: ", W)
	//} else {
	fmt.Println("Current Average: ", S/W)
	//}
}

func watchBarrier() {
	log.Println("WAiting on Barrier")
	if err := distb.Barrier.Wait(); err != nil {
		log.Fatalf("could not wait on barrier (%v)", err)
	}
	rwmutex.Lock()
	PushSumActive = false
	rwmutex.Unlock()
	log.Println("Ending PushSum, Converged: ", S/W)

}

func getCurrMaxTraffic() float64 {
	max, err := strconv.ParseFloat(ThisNode.ID, 64)
	if err != nil {
		log.Println("float conversion error")
	}
	return max
}

var (
	//ThisNode local attributes of the node
	ThisNode      distb.Node
	requests      chan distb.Message
	Logger        *log.Logger
	S             float64
	W             float64
	startpush     bool
	PushSumActive bool
	rwmutex       sync.RWMutex
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
		//time.Sleep(30 * time.Second)
		//pushSum(0, 0)
	}
	<-notListening
}
