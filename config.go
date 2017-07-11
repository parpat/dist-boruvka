package distboruvka

import (
	"bytes"
	"io/ioutil"
	"log"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/awalterschulze/gographviz"
	v3 "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/contrib/recipes"
	"golang.org/x/net/context"
)

//ETCDEndpoint to etcd clusters
const ETCDEndpoint string = "http://172.17.0.1:2379"

//GetEdgesFromFile config
func GetEdgesFromFile(fname string, id string) (Edges, map[string]*Edge) {
	rawContent, err := ioutil.ReadFile(fname)
	if err != nil {
		log.Fatal(err)
	}

	strs := strings.Split(string(rawContent), "\n")

	//wakeup, err := strconv.Atoi(strs[0])
	//pushSumStart := strs[0]
	//if err != nil {
	//	log.Print(err)
	//}
	//fmt.Printf("pushSumStart: %s\n", pushSumStart)

	strs = strs[1:]
	var edges Edges
	adjmap := make(map[string]*Edge)
	for _, line := range strs {
		if line != "" {
			vals := strings.Split(line, " ")
			s := vals[0]
			d := vals[1]
			w, _ := strconv.Atoi(vals[2])
			if s == id {
				edge := Edge{SE: "Basic", Weight: w, AdjNodeID: d, Origin: s}
				edges = append(edges, edge)
				adjmap[d] = &edge
			} else if d == id {
				edge := Edge{SE: "Basic", Weight: w, AdjNodeID: s, Origin: d}
				edges = append(edges, edge)
				adjmap[s] = &edge
			}
			//fmt.Printf("My edge %v\n", edges)
		}
	}

	sort.Sort(edges)

	return edges, adjmap
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

func GetGraphDOTFile(fname string, id string) (Edges, map[string]*Edge) {
	graphstr, err := ioutil.ReadFile(fname)
	if err != nil {
		log.Fatal(err)
	}

	log.Print(string(graphstr))

	g, err := gographviz.Read(graphstr)
	if err != nil {
		log.Fatal(err)
	}
	var edges Edges
	adjmap := make(map[string]*Edge)
	for _, e := range g.Edges.Edges {
		eID, err := strconv.Atoi(e.Attrs["label"])
		if err != nil {
			log.Print(err)
			eID = 0
		}

		eSrci, err := strconv.Atoi(e.Src)
		if err != nil {
			log.Fatal(err)
		}

		eSrci += 2
		eSrc := strconv.Itoa(eSrci)

		eDsti, err := strconv.Atoi(e.Dst)
		if err != nil {
			log.Fatal(err)
		}
		eDsti += 2
		eDst := strconv.Itoa(eDsti)

		if eSrc == id {
			edge := Edge{SE: "Basic", Weight: eID, AdjNodeID: eDst, Origin: eSrc}
			edges = append(edges, edge)
			adjmap[eDst] = &edge
		} else if eDst == id {
			edge := Edge{SE: "Basic", Weight: eID, AdjNodeID: eSrc, Origin: eDst}
			edges = append(edges, edge)
			adjmap[eSrc] = &edge
		}

	}

	sort.Sort(edges)
	return edges, adjmap

}
