package main

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	stats "github.com/montanaflynn/stats"

	distb "github.com/parpat/distboruvka"
)

//GATEWAY is the last octet of the docker subnetwork
const GATEWAY string = "1"
const PushSumIterations = 30

var psData [PushSumIterations]float64

func processMessages(reqs chan distb.Message) {
	for m := range reqs {
		//update edge message counter
		if m.SourceID != "gateway" {
			//fmt.Println("Received From: ", m.SourceID)
			updateMessageCounter(m.SourceID)
		}

		switch m.Type {
		case "ReqAdjEdges":
			go sendEdges()

		case "MSTBranch":
			go markBranch(m.Edges[0])

		case "PushSum":
			TnumPushMsg++
			pushSum(m.S, m.W)

		case "DumTraffic":
			go dumTraffic(m.SourceID)

		case "TrafficData":
			go sendTrafficData()

		case "IsHighTraffic":
			log.Println("sending HIGH TRAFFIC REPLY")
			distb.SendMessage(distb.Message{SourceID: ThisNode.ID, HighTraffic: IsHighTraffic, Type: "IsHighTrafficReply"}, ThisNode.ID, m.SourceID)

		case "IsHighTrafficReply":
			adjTrafficChan <- m.HighTraffic

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
func dumTraffic(inID string) {
	//Choose random neighbor
	rand.Seed(time.Now().UnixNano())
	//ThisNode.RLock()
	//defer ThisNode.RUnlock()
	randAdjEdge := (*ThisNode.AdjacencyList)[0]
	if len(*ThisNode.AdjacencyList) > 1 {
		randAdjEdge = (*ThisNode.AdjacencyList)[rand.Intn(len(*ThisNode.AdjacencyList))]
	}

	/*	if len(*ThisNode.AdjacencyList) > 1 {
			for {
				randAdjEdge = (*ThisNode.AdjacencyList)[rand.Intn(len(*ThisNode.AdjacencyList))]
				if randAdjEdge.AdjNodeID != inID {
					break
				}
			}
		} else { // drop message
			return
		}*/

	//fmt.Println("Dum message sent to: ", randAdjEdge.AdjNodeID)
	time.Sleep(time.Millisecond * 20)
	randAdjEdge.Send(&distb.Message{Type: "DumTraffic"})
	updateMessageCounter(randAdjEdge.AdjNodeID)

}

func sendTrafficData() {
	ThisNode.RLock()
	msg := distb.Message{BufferA: TnumMsg, BufferB: TnumPushMsg, HighTraffic: IsHighTraffic, Edges: HighRiskEdges}
	ThisNode.RUnlock()
	distb.SendMessage(msg, ThisNode.ID, GATEWAY)
}

func dummyInjector() {
	for {
		time.Sleep(time.Millisecond * 5000)
		dumTraffic("999")
	}

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
var convergenceDiff = 0.000000001
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
		psStateLock.Lock()
		if Ti == 0 {
			Ti, BEi = getCurrMaxTraffic()
		} else if pushSumCounter >= PushSumIterations {
			Ti, BEi = getCurrMaxTraffic()
			pushSumCounter = 0
			log.Println("Push Sum counter reset")
		}
		PushSumActive = true

		//S = Ti max traffic edge Edge
		S = float64(TnumMsg) //total traffic on node
		W = 1
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
	//time.Sleep(time.Millisecond * 1)
	fmt.Println("PUSH-SUM Send to: ", randAdjEdge.AdjNodeID)
	randAdjEdge.Send(msgPush)
	updateMessageCounter(randAdjEdge.AdjNodeID)

	S += sh
	W += wh

	fmt.Println("Current Average: ", S/W)

}

func watchBarrier() {
	log.Println("Waiting on Barrier")
	if err := distb.Barrier.Wait(); err != nil {
		log.Fatalf("could not wait on barrier (%v)", err)
	}
	psStateLock.Lock()
	defer psStateLock.Unlock()
	PushSumActive = false
	pushSumCounter++
	psData[pushSumCounter-1] = S / W

	if pushSumCounter >= PushSumIterations {
		calcStats()
		findHighRiskEdges()
	} else {
		log.Println("Did not calc stats; counter: ", pushSumCounter)
		highTrafficCheck = true
	}

	log.Println("Ending PushSum, Converged: ", S/W)
	log.Println("Traffic at convergence: ", Ti)
	log.Println("Total Num messages: ", TnumMsg)

}

func calcStats() {
	//psStateLock.RLock()
	//defer psStateLock.RUnlock()

	statsData := stats.Float64Data(psData[:])
	stdDev, err := statsData.StandardDeviation()
	fmt.Printf("Std_Dev: %.3f\n", stdDev)
	if err != nil {
		log.Println("Couldnt calc StandardDeviation")
	}
	mean, _ := statsData.Mean()
	fmt.Printf("Mean: %.3f\n", mean)

	threshold := (2.0 * stdDev) + mean
	lowThreshold := (1.1 * stdDev) + mean

	ThisNode.Lock()
	defer ThisNode.Unlock()

	if float64(TnumMsg) >= threshold {
		fmt.Printf("High traffic! Node: %v Edge: %v\n", Ti, BEi)
		IsHighTraffic = true

	} else if float64(TnumMsg) >= (lowThreshold) {
		fmt.Printf("Unlikely Bottleneck! Node: %v Edge: %v\n", Ti, BEi)
		IsHighTraffic = true
	}

	highTrafficCheck = true

}

func getCurrMaxTraffic() (float64, int) {
	var max = 0
	var edgeWeight = 0
	ThisNode.Lock()
	defer ThisNode.Unlock()
	TnumMsg = 0
	for _, e := range ThisNode.AdjacencyMap {
		TnumMsg += e.MessageCount
		log.Printf("Traffic at link TO %s IS %d\n", e.AdjNodeID, e.MessageCount)
		if max < e.MessageCount {
			max = e.MessageCount
			edgeWeight = e.Weight
		}
	}
	log.Println("Max Traffic: ", max)
	log.Println("Node Total: ", TnumMsg)
	return float64(max), edgeWeight
}

func findHighRiskEdges() {
	ThisNode.RLock()
	isThisHT := IsHighTraffic
	ThisNode.RUnlock()
	if isThisHT {
		for _, e := range *ThisNode.AdjacencyList {
			e.Send(&distb.Message{SourceID: ThisNode.ID, Type: "IsHighTraffic"})
			log.Println("--> HIGH TRAFFIC CHECK --->", e.AdjNodeID)
			isAdjNodeHT := <-adjTrafficChan
			log.Println("<-- HIGH TRAFFIC CHECK REPLY<---", isAdjNodeHT)
			if isAdjNodeHT {
				HighRiskEdges = append(HighRiskEdges, e)
			}

		}
	}
}

var (
	//ThisNode local attributes of the node
	ThisNode         *distb.Node
	requests         chan distb.Message
	Logger           *log.Logger
	S                float64
	W                float64
	Ti               float64
	BEi              int
	TnumMsg          int
	HighRiskEdges    distb.Edges
	startpush        bool
	PushSumActive    bool
	psStateLock      = &sync.RWMutex{}
	IsHighTraffic    bool
	highTrafficCheck bool
	adjTrafficChan   chan bool
	TnumPushMsg      int
)

func init() {
	hostName, hostIP := distb.GetHostInfo()
	octets := strings.Split(hostIP, ".")
	log.Printf("My ID is: %s\n", octets[3])
	nodeID := octets[3]
	edges, adjMap := distb.GetGraphDOTFile("/home/parth/workspace/networkgen/graph.dot", nodeID)

	ThisNode = &distb.Node{
		ID:            nodeID,
		Name:          hostName,
		AdjacencyList: &edges,
		AdjacencyMap:  adjMap}

	log.Printf("ADJLIST: %v\n", *ThisNode.AdjacencyList)

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

	//channel for findHighRiskEdges that concurrently queries adj nodes
	//and waits for reply
	adjTrafficChan = make(chan bool)
}

func main() {
	requests = make(chan distb.Message, 25)
	notListening := make(chan bool)

	go distb.ListenAndServeTCP(notListening, requests)
	//Process incomming messages
	go processMessages(requests)

	go distb.SetNodeInfo(ThisNode.Name, ThisNode.ID)

	time.Sleep(time.Second * 100)
	//go dummyInjector() // Consistently generate messages
	dumTraffic("999") // For starting with one message
	<-notListening
}
