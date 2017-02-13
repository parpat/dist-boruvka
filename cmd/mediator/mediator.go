package main

import (
	"fmt"

	distb "github.com/parpat/distboruvka"
)

var (
	requests chan distb.Message
)

func initBoruvka() {
	nodes := distb.GetNodes()
	for _, n := range nodes {

		distb.SendMessage(distb.Message{Type: "ReqAdjEdges"}, n.ID)
		m := <-requests
		fmt.Printf("%s's min Edge: -%v> %v\n ", m.Edges[0].Origin, m.Edges[0].Weight, m.Edges[0].AdjNodeID)
	}

}

func main() {

	notListening := make(chan bool)

	requests = make(chan distb.Message)

	go distb.ListenAndServeTCP(notListening, requests)

	go initBoruvka()

	<-notListening

}
