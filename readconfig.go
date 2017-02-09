package distboruvka

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"os/exec"
	"sort"
	"strconv"
	"strings"
)

//SUBNET of docker network
const SUBNET string = "172.17.0."

//PORT is main comm port
const PORT string = "7575"

//GetEdgesFromFile config
func GetEdgesFromFile(fname string, id int) (Edges, int) {
	rawContent, err := ioutil.ReadFile(fname)
	if err != nil {
		log.Fatal(err)
	}

	strs := strings.Split(string(rawContent), "\n")

	wakeup, err := strconv.Atoi(strs[0])
	if err != nil {
		log.Print(err)
	}
	fmt.Printf("wakeup: %v\n", wakeup)

	strs = strs[1:]
	var myedges Edges
	for _, edge := range strs {
		if edge != "" {
			vals := strings.Split(edge, " ")
			s, _ := strconv.Atoi(vals[0])
			d, _ := strconv.Atoi(vals[1])
			w, _ := strconv.Atoi(vals[2])
			if s == id {
				myedges = append(myedges, Edge{SE: "Basic", Weight: w, AdjNodeID: d, Origin: s})
			} else if d == id {
				myedges = append(myedges, Edge{SE: "Basic", Weight: w, AdjNodeID: s, Origin: d})
			}
			//fmt.Printf("My edge %v\n", myedges)
		}
	}

	sort.Sort(myedges)

	return myedges, wakeup
}

func GetHostInfo() (string, string) {
	HostIP, err := exec.Command("hostname", "-i").Output()
	if err != nil {
		log.Fatal(err)
	}
	HostIP = bytes.TrimSuffix(HostIP, []byte("\n"))

	HostName, err := exec.Command("hostname").Output()
	if err != nil {
		log.Fatal(err)
	}
	HostName = bytes.TrimSuffix(HostName, []byte("\n"))

	return string(HostName), string(HostIP)
}
