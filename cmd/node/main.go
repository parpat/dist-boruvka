package main

import (
	"log"
	"net"
)

func processMessage(reqs chan *Message) {
	for m := range reqs {
		if m.Type == "ReqAdjEdges" {

		}
	}
}

func main() {

	notListening := make(chan bool)
	go func(nl chan bool) {
		defer func() {
			nl <- true
		}()
		l, err := net.Listen("tcp", PORT)
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

			// Handle the connection in a new goroutine.
			go serveConn(conn, requests)
		}
	}(notListening)

	//Process incomming messages
	go processMessage(requests)

	<-notListening
}
