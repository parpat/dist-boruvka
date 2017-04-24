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
const PushSumIterations = 30

func processMessages(reqs chan distb.Message) {
	for m := range reqs {
		//update edge message counter
		if m.SourceID != "gateway" {
			//fmt.Println("Received From: ", m.SourceID)
			updateMessageCounter(m.SourceID)
		}

		switch m.Type {
		case "ReqAdjEdges":
			sendEdges()

		case "MSTBranch":
			markBranch(m.Edges[0])

		case "PushSum":
			pushSum(m.S, m.W)

		case "DumTraffic":
			go dumTraffic()
		}
	}

}

//TODO:Send sorted edges
func sendEdges() {
	msg := distb.Message{Edges: *ThisNode.AdjacencyList}
	distb.SendMessage(msg, ThisNode.ID, GATEWAY)
}

func markBranch(e distb.Edge) {
	idx := ThisNode.FindEdgeIdx(e.Weight)
	(*ThisNode.AdjacencyList)[idx].SE = "Branch"
	fmt.Printf("%v is now a Branch\n", e.Weight)
}

//Keep pushing dummy traffic to random nodes
func dumTraffic() {
	//Choose random neighbor
	rand.Seed(time.Now().UnixNano())
	randAdjEdge := (*ThisNode.AdjacencyList)[rand.Intn(len(*ThisNode.AdjacencyList))]
	//fmt.Println("Dum message to: ", randAdjEdge.AdjNodeID)
	time.Sleep(time.Millisecond * 200)
	randAdjEdge.Send(&distb.Message{Type: "DumTraffic"})
	updateMessageCounter(randAdjEdge.AdjNodeID)

}

func updateMessageCounter(adjnode string) {
	ThisNode.Lock()
	ThisNode.AdjacencyMap[adjnode].MessageCount++
	ThisNode.Unlock()
}

//pushSum protocol is a form of network aggregation used to calculate
//the average value between nodes in a distributed fashion. The propagation
//of data is achieved using simple gossip protocol by selecting a uniformly random neighbor
//to send data to.
var lastRatio = 0.0
var ratioConvergeCount = 0
var converged = false
var convergenceDiff = 0.00000001
var pushSumCounter = 0 //keeps track of the number of samples

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

		//Keep same traffic snapshot until number of samples complete
		if Ti == 0 {
			Ti = getCurrMaxTraffic()
		} else if pushSumCounter >= PushSumIterations {
			Ti = getCurrMaxTraffic()
			pushSumCounter = 0
			log.Println("Push Sum counter reset")
		}

		S = Ti
		W = 1
		psStateLock.Lock()
		PushSumActive = true
		psStateLock.Unlock()
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
			distb.SendMessage(distb.Message{Type: "Average", Avg: ratio}, ThisNode.ID, GATEWAY)
			return
		}
	} else {
		ratioConvergeCount = 0
	}

	//Choose random neighbor
	rand.Seed(time.Now().UnixNano())
	randAdjEdge := (*ThisNode.AdjacencyList)[rand.Intn(len(*ThisNode.AdjacencyList))]

	//Send pair (0.5S, 0.5W)
	sh := S / 2
	wh := W / 2
	msgPush := &distb.Message{Type: "PushSum", S: sh, W: wh, SourceID: ThisNode.ID}
	time.Sleep(time.Millisecond * 5)
	fmt.Println("Sent to: ", randAdjEdge.AdjNodeID)
	randAdjEdge.Send(msgPush)
	updateMessageCounter(randAdjEdge.AdjNodeID)

	S += sh
	W += wh

	fmt.Println("Current Average: ", S/W)

}

func watchBarrier() {
	log.Println("WAiting on Barrier")
	if err := distb.Barrier.Wait(); err != nil {
		log.Fatalf("could not wait on barrier (%v)", err)
	}
	psStateLock.Lock()
	PushSumActive = false
	psStateLock.Unlock()
	log.Println("Ending PushSum, Converged: ", S/W)
	log.Println("Traffic at convergence: ", Ti)
	pushSumCounter++

}

func getCurrMaxTraffic() float64 {
	var max = 0
	ThisNode.RLock()
	defer ThisNode.RUnlock()
	for _, e := range ThisNode.AdjacencyMap {
		if max < e.MessageCount {
			max = e.MessageCount
		}
	}
	log.Println("Max Traffic: ", max)
	return float64(max)
}

var (
	//ThisNode local attributes of the node
	ThisNode      *distb.Node
	requests      chan distb.Message
	Logger        *log.Logger
	S             float64
	W             float64
	Ti            float64
	startpush     bool
	PushSumActive bool
	psStateLock   = &sync.RWMutex{}
)

func init() {
	hostName, hostIP := distb.GetHostInfo()
	octets := strings.Split(hostIP, ".")
	fmt.Printf("My ID is: %s\n", octets[3])
	nodeID := octets[3]
	edges, adjMap := distb.GetEdgesFromFile("boruvka.conf", nodeID)

	ThisNode = &distb.Node{
		ID:            nodeID,
		Name:          hostName,
		AdjacencyList: &edges,
		AdjacencyMap:  adjMap}

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
	//if pushSumStart == nodeID {
	//	startpush = true
	//}
}

func main() {
	requests = make(chan distb.Message, 5)
	notListening := make(chan bool)

	go distb.ListenAndServeTCP(notListening, requests)
	//Process incomming messages
	go processMessages(requests)

	go distb.SetNodeInfo(ThisNode.Name, ThisNode.ID)

	time.Sleep(time.Second * 7)
	dumTraffic()

	<-notListening
}
