package main

import (
	"encoding/gob"
	"fmt"
	"log"
	"net"

	"github.com/parpat/distboruvka"
)

const PORT string = "7575"

func initBoruvka() {

	conntwo, err := net.Dial("tcp", "172.17.0.2:9595")
	if err != nil {
		log.Println(err)
		//log.Printf("conn null? %v\n", conntwo == nil)
	} else {
		enc := gob.NewEncoder(conntwo)
		err = enc.Encode(distboruvka.Message{Type: "ReqAdjEdges"})
		log.Println("ReqAdjEdges sent")
		if err != nil {
			log.Fatal(err)
		}
	}
}

func serveConn(c net.Conn, reqs chan distboruvka.Message) {
	var msg distboruvka.Message
	dec := gob.NewDecoder(c)
	err := dec.Decode(msg)
	if err != nil {
		fmt.Print(err)
	}
	reqs <- msg
	fmt.Printf("Receieved message: %v\n", msg.Type)
}

func main() {

	notListening := make(chan bool)

	requests := make(chan distboruvka.Message)

	go func(nl chan bool) {
		defer func() {
			nl <- true
		}()
		l, err := net.Listen("tcp", ":"+PORT)
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
