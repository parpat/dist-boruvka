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

	"golang.org/x/net/context"

	"github.com/coreos/etcd/client"
)

//ETCDEndpoint to etcd clusters
const ETCDEndpoint string = "http://172.17.0.1:2379"

//GetEdgesFromFile config
func GetEdgesFromFile(fname string, id string) Edges {
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

	return myedges
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

//API to interact with etcd
var (
	kapi client.KeysAPI
)

func init() {
	cfg := client.Config{
		Endpoints: []string{ETCDEndpoint},
		Transport: client.DefaultTransport,
		//Target endpoint timeout
		HeaderTimeoutPerRequest: time.Second,
	}
	c, err := client.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	kapi = client.NewKeysAPI(c)
}

//SetNodeInfo sets this node's current host(container)name and last IP octet
func SetNodeInfo(name, id string) {
	_, err := kapi.Set(context.Background(), "/nodes/"+name, id, nil)
	if err != nil {
		log.Fatal(err)
	} else {
		log.Println("NodeInfo  registered to ETCD ")
	}
}

//GetNodes retrieves the nodes list from etcd
func GetNodes() []Node {
	var nodes []Node

	getopt := &client.GetOptions{Recursive: true, Sort: true, Quorum: true}
	resp, err := kapi.Get(context.Background(), "/nodes", getopt)
	if err != nil {
		log.Println("Failed to obtain node: ", err)
	} else {
		log.Println("Refreshed node list")
		if (resp.Node).Nodes != nil {
			//Nodes in type etcd/client.Response refer to etcd entries with KV
			for _, node := range resp.Node.Nodes {
				cName := strings.TrimPrefix(node.Key, "/nodes/")
				nodes = append(nodes, Node{Name: cName, ID: node.Value})
				log.Printf("Key: %q  Value: %q\n", cName, node.Value)
			}

		}
	}
	return nodes
}
