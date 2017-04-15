package distboruvka

import (
	"encoding/gob"
	"fmt"
	"log"
	"net"
)

//SUBNET of docker network
const SUBNET string = "172.17.0."

//PORT is main comm port
const PORT string = "7575"

//Message is the template for commuication
type Message struct {
	Type     string
	SourceID string
	Edges    Edges
	S        float64
	W        float64
	Avg      float64
}

//SendMessage sends the message to a destination in the docker network
func SendMessage(m Message, node string) {
	cliconn, err := net.Dial("tcp", SUBNET+node+":"+PORT)
	if err != nil {
		log.Println(err)
		//log.Printf("conn null? %v\n", conntwo == nil)
	} else {
		enc := gob.NewEncoder(cliconn)
		if err = enc.Encode(m); err != nil {
			log.Println(err)
		} else {
			log.Printf("%v sent\n", m.Type)
		}
	}
}

//ServeConn receives and decodes gob Message
func ServeConn(c net.Conn, reqs chan Message) {
	defer c.Close()
	var msg Message
	dec := gob.NewDecoder(c)
	err := dec.Decode(&msg)
	if err != nil {
		fmt.Print(err)
	}
	//log.Println(c.RemoteAddr())
	reqs <- msg
	//fmt.Printf("Receieved message: %v\n", msg.Type)
}

//ListenAndServeTCP listens for tcp requests and serves connections by
//putting messages in the request queue
func ListenAndServeTCP(listening chan bool, requests chan Message) {
	defer func() {
		listening <- false
	}()
	l, err := net.Listen("tcp", ":"+PORT)
	//fmt.Println("Listening")
	log.Println("Listening")
	if err != nil {
		log.Fatal(err)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Fatal(err)
		}
		//fmt.Println("serving Conn")

		// Handle the connection in a new goroutine.
		go ServeConn(conn, requests)
	}
}
