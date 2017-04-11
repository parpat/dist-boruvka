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
	"time"

	v3 "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/contrib/recipes"
	"golang.org/x/net/context"
)

//ETCDEndpoint to etcd clusters
const ETCDEndpoint string = "http://172.17.0.1:2379"

//GetEdgesFromFile config
func GetEdgesFromFile(fname string, id string) (Edges, string) {
	rawContent, err := ioutil.ReadFile(fname)
	if err != nil {
		log.Fatal(err)
	}

	strs := strings.Split(string(rawContent), "\n")

	//wakeup, err := strconv.Atoi(strs[0])
	pushSumStart := strs[0]
	//if err != nil {
	//	log.Print(err)
	//}
	fmt.Printf("pushSumStart: %s\n", pushSumStart)

	strs = strs[1:]
	var myedges Edges
	for _, edge := range strs {
		if edge != "" {
			vals := strings.Split(edge, " ")
			s := vals[0]
			d := vals[1]
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

	return myedges, pushSumStart
}

//GetHostInfo get the current node's IP and hostname
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

var (
	cli     *v3.Client
	Barrier *recipe.Barrier
)

func init() {
	cfg := v3.Config{
		Endpoints: []string{ETCDEndpoint},
		//Target endpoint timeout
		DialTimeout: time.Second * 5,
	}
	var err error
	cli, err = v3.New(cfg)
	if err != nil {
		log.Fatal(err)
	}

	Barrier = recipe.NewBarrier(cli, "DistBarrier")
}

//SetNodeInfo sets this node's current host(container)name and last IP octet
func SetNodeInfo(name, id string) {
	_, err := cli.Put(context.Background(), "/nodes/"+name, id)
	if err != nil {
		log.Fatal(err)
	} else {
		log.Println("NodeInfo  registered to ETCD ")
	}
}

//GetNodes retrieves the nodes list from etcd
func GetNodes() []Node {
	var nodes []Node

	resp, err := cli.Get(context.Background(), "/nodes", v3.WithPrefix())
	if err != nil {
		log.Println("Failed to obtain node: ", err)
	} else {
		log.Println("Refreshed node list")
		if resp.Kvs == nil {
			log.Println("IsNil")
		}

		for _, ev := range resp.Kvs {
			cName := bytes.TrimPrefix(ev.Key, []byte("/nodes/"))
			nodes = append(nodes, Node{Name: string(cName[:]), ID: string(ev.Value[:])})
			log.Printf("Key: %s  Value: %q\n", ev.Key, ev.Value)
		}
	}

	return nodes
}

//Wait on Barrier
