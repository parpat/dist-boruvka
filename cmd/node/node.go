package main

import (
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"

	"github.com/parpat/distboruvka"
)

//Node is the current instance
type Node struct {
	ID            int
	adjacencyList *distboruvka.Edges
}

func processMessage(reqs chan *distboruvka.Message) {
	for m := range reqs {
		fmt.Println("request received")
		if m.Type == "ReqAdjEdges" {
			sendEdges()
		}
	}
}

func sendEdges() {
	conntwo, err := net.Dial("tcp", distboruvka.SUBNET+strconv.Itoa(1)+":7575")
	if err != nil {
		log.Println(err)
		log.Printf("conn null? %v\n", conntwo == nil)
	} else {
		enc := gob.NewEncoder(conntwo)
		err = enc.Encode((*ThisNode.adjacencyList)[0])
		if err != nil {
			log.Fatal(err)
		}
	}
	log.Println("message sent to:" + distboruvka.SUBNET + strconv.Itoa(1) + ":7575")
}

var (
	HostName string
	HostIP   string
	ThisNode Node
	requests chan *distboruvka.Message
	Logger   *log.Logger
)

func init() {
	HostName, HostIP = distboruvka.GetHostInfo()
	octets := strings.Split(HostIP, ".")
	fmt.Printf("My ID is: %s\n", octets[3])
	nodeID, err := strconv.Atoi(octets[3])
	edges, _ := distboruvka.GetEdgesFromFile("boruvka.conf", nodeID)
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

func serveConn(c net.Conn, reqs chan *distboruvka.Message) {
	defer c.Close()
	var resp distboruvka.Message
	dec := gob.NewDecoder(c)
	err := dec.Decode(&resp)
	if err != nil {
		Logger.Print(err)
	}

	reqs <- &resp
	fmt.Println("placed in request queue")
}

func main() {
	requests = make(chan *distboruvka.Message, 50)
	notListening := make(chan bool)
	go func(nl chan bool) {
		defer func() {
			nl <- true
		}()
		l, err := net.Listen("tcp", ":9595")
		//fmt.Println("Listening")
		log.Println("Listening")
		if err != nil {
			log.Fatal(err)
		}

		for {
			conn, err := l.Accept()
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println("serving Conn")

			// Handle the connection in a new goroutine.
			go serveConn(conn, requests)
		}
	}(notListening)

	//Process incomming messages
	go processMessage(requests)

	<-notListening
}
