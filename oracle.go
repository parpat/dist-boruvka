package main

import (
	"log"
	"net"
	"time"
)

const PORT string = "8585"

//Message
type Message struct {
	Type     string
	SourceID int
	E        Edge
}

func processMessage(reqs chan *Message) {

}

func main() {
	requests = make(chan *Message, 50)

	//Initialize Server
	notListening := make(chan bool)
	go func(nl chan bool) {
		defer func() {
			nl <- true
		}()
		l, err := net.Listen("tcp", PORT)
		log.Println("Listening")
		if err != nil {
			log.Fatal(err)
		}

		for {
			conn, err := l.Accept()
			if err != nil {
				log.Println(err)
			}

			// Handle the connection in a new goroutine.
			go serveConn(conn, requests)
		}
	}(notListening)

	go processMessage(requests)

	time.Sleep(time.Second * 10)

	//Wait until listening routine sends signal
	<-notListening
}
