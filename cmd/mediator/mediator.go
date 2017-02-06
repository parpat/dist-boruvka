package main

import (
	"encoding/gob"
	"fmt"
	"log"
	"net"

	"github.com/parpat/distboruvka"
)

const PORT string = "7575"

func main() {

	notListening := make(chan bool)
	//log.Printf("STATUS: %v  INBRANCH: %v FCOUNT: %v", ThisNode.SN, (*ThisNode.inBranch).Weight, ThisNode.findCount)
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
			conn, err := l.Accept()
			if err != nil {
				log.Fatal(err)
			}
			// Handle the connection in a new goroutine.
			conn, err = l.Accept()
			fmt.Printf("Got Conn")
			if err != nil {
				log.Fatal(err)
			}

			var resp distboruvka.Edge
			dec := gob.NewDecoder(conn)
			err = dec.Decode(&resp)
			if err != nil {
				fmt.Print(err)
			}

			fmt.Printf("First Edge: %v\n", resp.Weight)
		}
		nl <- true
	}(notListening)

	conntwo, err := net.Dial("tcp", "172.17.0.2:9595")
	if err != nil {
		log.Println(err)
		log.Printf("conn null? %v\n", conntwo == nil)
	} else {
		enc := gob.NewEncoder(conntwo)
		err = enc.Encode(distboruvka.Message{Type: "ReqAdjEdges"})
		log.Println("ReqAdjEdges sent")
		if err != nil {
			log.Fatal(err)
		}
	}

	<-notListening

}
