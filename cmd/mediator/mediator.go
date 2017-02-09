package main

import (
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"strconv"

	pdb "github.com/parpat/distboruvka"
)

var (
	requests chan Message
)

type Message pdb.Message

func initBoruvka() {
	//rresp := make(chan)
	for i := 2; i <= 5; i++ {
		sendMessage(Message{Type: "ReqAdjEdges"}, i)
		m := <-requests
		fmt.Printf("%d's min Edge: -%v> %v\n ", i, m.Edges[0].Weight, m.Edges[0].AdjNodeID)
	}

}

func sendMessage(m Message, node int) {
	cliconn, err := net.Dial("tcp", pdb.SUBNET+strconv.Itoa(node)+":9595") //+pdb.PORT)
	if err != nil {
		log.Println(err)
		//log.Printf("conn null? %v\n", conntwo == nil)
	} else {
		enc := gob.NewEncoder(cliconn)
		if err = enc.Encode(m); err != nil {
			log.Println(err)
		} else {
			log.Printf("%v sent\n", m.Type)
		}
	}
}

func serveConn(c net.Conn, reqs chan Message) {
	var msg Message
	dec := gob.NewDecoder(c)
	err := dec.Decode(&msg)
	if err != nil {
		fmt.Print(err)
	}
	reqs <- msg
	fmt.Printf("Receieved message: %v\n", msg.Type)
}

func main() {

	notListening := make(chan bool)

	requests = make(chan Message)

	go func(nl chan bool) {
		defer func() {
			nl <- true
		}()
		l, err := net.Listen("tcp", ":"+pdb.PORT)
		fmt.Println("Listening")
		if err != nil {
			log.Fatal(err)
		}

		for {
			// Handle the connection in a new goroutine.
			conn, err := l.Accept()
			fmt.Println("Got Conn")
			if err != nil {
				log.Fatal(err)
			}
			go serveConn(conn, requests)
		}
	}(notListening)

	go initBoruvka()

	<-notListening

}
