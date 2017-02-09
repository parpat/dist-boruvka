package main

import (
	"fmt"
	"strconv"

	distb "github.com/parpat/distboruvka"
)

var (
	requests chan distb.Message
)

func initBoruvka() {
	//rresp := make(chan)
	for i := 2; i <= 5; i++ {
		distb.SendMessage(distb.Message{Type: "ReqAdjEdges"}, strconv.Itoa(i))
		m := <-requests
		fmt.Printf("%d's min Edge: -%v> %v\n ", m.Edges[0].Origin, m.Edges[0].Weight, m.Edges[0].AdjNodeID)
	}

}

func main() {

	notListening := make(chan bool)

	requests = make(chan distb.Message)

	go distb.ListenAndServeTCP(notListening, requests)

	go initBoruvka()

	<-notListening

}
