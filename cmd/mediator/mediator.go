package main

import (
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"time"

	distb "github.com/parpat/distboruvka"
	"github.com/parpat/distboruvka/quickFind"
)

var (
	requests chan distb.Message
)

func initBoruvka() {

	nodes := distb.GetNodes()
	//1   Initialize a forest T to be a set of one-vertex trees, one for each vertex of the graph.
	forest := quickFind.InitializeQF(len(nodes))
	T := *new(distb.Edges)
	//2   While T has more than one component:
	for comps := forest.GetComponents(); len(comps) > 1; comps = forest.GetComponents() {
		//if
		//fmt.Printf("Components: %v\n", comps)
		// 3 For each component C of T:
		for c := range comps {
			// 4 Begin with an empty set of edges S
			S := *new(distb.Edges)
			// 5 For each vertex v in C:
			fmt.Printf("Iterating component: %v\n", c)
			for _, n := range nodes {
				id, _ := strconv.Atoi(n.ID)
				if forest.ID[id-2] == c {
					distb.SendMessage(distb.Message{Type: "ReqAdjEdges"}, n.ID)
					m := <-requests
					// 6 Find the cheapest edge from v to a vertex outside of C, and add it to S
					for _, cedge := range m.Edges {
						currnode, _ := strconv.Atoi(cedge.Origin)
						adjnode, _ := strconv.Atoi(cedge.AdjNodeID)
						if forest.Find(currnode-2) != forest.Find(adjnode-2) {
							S = append(S, cedge)
							break
						}
					}
				}
				//fmt.Printf("%s's min Edge: -%v> %v\n ", m.Edges[0].Origin, m.Edges[0].Weight, m.Edges[0].AdjNodeID)
			}
			if S != nil {
				sort.Sort(S)
				fmt.Printf("S: %v\n", S)
				T = append(T, S[0])
			}

		}
		// 8 Combine trees connected by edges to form bigger components
		for _, combEdge := range T {
			currnode, _ := strconv.Atoi(combEdge.Origin)
			currnode -= 2
			adjnode, _ := strconv.Atoi(combEdge.AdjNodeID)
			adjnode -= 2
			if forest.Find(currnode) != forest.Find(adjnode) {
				forest.Union(currnode, adjnode)
				fmt.Printf("Combined nodes %v %v\n", currnode, adjnode)
			}
		}
	}

	removeDuplicates(&T)
	sort.Sort(T)

	//Sending mst branches to their endpoints
	for _, mste := range T {
		mstBranchmsg := distb.Message{Type: "MSTBranch", Edges: distb.Edges{mste}}
		distb.SendMessage(mstBranchmsg, mste.Origin)
		distb.SendMessage(mstBranchmsg, mste.AdjNodeID)
		fmt.Printf("MST Edge W: %d\n", mste.Weight)
	}
	//return T

}

func removeDuplicates(dup *distb.Edges) {
	found := make(map[int]bool)
	j := 0
	for i, de := range *dup {
		if !found[de.Weight] {
			found[de.Weight] = true
			(*dup)[j] = (*dup)[i]
			j++
		}
	}
	*dup = (*dup)[:j]
}

//calcAverage initiated the push-sum protocol among the nodes
//the results should be send back to the mediator after convergence
func calcAverage(csvFile *os.File) {
	defer csvFile.Close()
	for i := 0; i < 50; i++ {
		distb.SendMessage(distb.Message{Type: "PushSum", S: 0, W: 0}, "4")
		m := <-requests
		avgstr := fmt.Sprintf("%.3f", m.Avg)
		time.Sleep(time.Millisecond * 5)
		log.Printf("Average at: %d  is %s\n", i, avgstr)
		csvFile.WriteString(avgstr + ",\n")
	}
}

func main() {

	notListening := make(chan bool)

	requests = make(chan distb.Message)

	go distb.ListenAndServeTCP(notListening, requests)

	//go initBoruvka()
	cF, err := os.Create("data.csv")
	if err != nil {
		log.Fatal("Cant open csv")
	}

	go calcAverage(cF)

	<-notListening
}
