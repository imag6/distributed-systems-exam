package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"

	dht "distributed-systems-exam/dht"
)

var hashTable map[int32]int32
var lock sync.Mutex

type Server struct {
	dht.UnimplementedDhtServiceServer
	Name     int32
	Port     int32
	PeerPort int32
}

func main() {
	args := os.Args[1:]
	if len(args) < 3 {
		fmt.Println("Arguments required: <name> <port> <peer address>")
		os.Exit(1)
	}
	name, err := strconv.Atoi(args[0])
	listenAddr, err1 := strconv.Atoi(args[1])
	peerPort, err2 := strconv.Atoi(args[2])

	if err != nil || err1 != nil || err2 != nil {
		fmt.Println("Arguments has to be Integers")
		os.Exit(1)
	}

	s := Server{Name: int32(name), Port: int32(listenAddr), PeerPort: int32(peerPort)}

	go s.Start()
	time.Sleep(5 * time.Second)

	hashTable = make(map[int32]int32)

	for {
		s.Heartbeat()
	}

}

func (ser *Server) Start() {
	lis, err := net.Listen("tcp", toAddr(ser.Port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	dht.RegisterDhtServiceServer(s, ser)
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func toAddr(port int32) string {
	return fmt.Sprintf("localhost:%v", port)
}

func (s *Server) Put(ctx context.Context, pair *dht.Pair) (*dht.Ack, error) {
	if checkInput(pair.IsClient, pair.Key, pair.Value) {
		//lock.Lock()
		nkey := pair.Key
		nvalue := pair.Value
		hashTable[nkey] = nvalue

		//defer lock.Unlock()

		connection, err := grpc.Dial(toAddr(s.PeerPort), grpc.WithInsecure())
		defer connection.Close()

		if err != nil {
			log.Printf("Could not get access: %v", err)
			if s.PeerPort == 8800 {
				s.PeerPort = 9000
				if s.Port == s.PeerPort {
					log.Fatalf("Only one server left, the application has crashed. ")
				}
				return &dht.Ack{WasSuccess: false}, nil
			} else {
				s.PeerPort = s.PeerPort - 100
				if s.Port == s.PeerPort {
					log.Fatalf("Only one server left, the application has crashed. ")
				}
				return &dht.Ack{WasSuccess: false}, nil
			}

		}

		client := dht.NewDhtServiceClient(connection)
		request := dht.Pair{IsClient: false, Key: pair.Key, Value: pair.Value}
		client.Put(ctx, &request)

		log.Printf("The put operation was successful. Key: %d, Value: %d \n", pair.Key, pair.Value)
		fmt.Println(" ")

		return &dht.Ack{WasSuccess: true}, nil
	}

	return &dht.Ack{WasSuccess: true}, nil
}

func checkInput(isClient bool, key int32, value int32) bool {

	if isClient {
		return true
	}

	if hashTable[key] == value {
		return false
	} else {
		return true
	}
}

func (s *Server) Get(ctx context.Context, key *dht.Key) (*dht.Value, error) {
	if key.IsClient {
		//lock.Lock()
		valueToBeReturned := hashTable[key.Key]
		log.Printf("GET: the value %d from the key %d", valueToBeReturned, key.Key)
		if valueToBeReturned < 1 {
			return &dht.Value{Value: 0}, nil
		}
		//lock.Unlock()
		return &dht.Value{Value: valueToBeReturned}, nil
	}
	return &dht.Value{Value: 0}, nil
}

func (ser *Server) Heartbeat() {
	connection, err := grpc.Dial(toAddr(ser.PeerPort), grpc.WithInsecure())
	defer connection.Close()

	if err != nil {
		log.Printf("could not get Access: %v", err)
		if ser.PeerPort == 8800 {
			ser.PeerPort = 9000
			return
		} else {
			ser.PeerPort = ser.PeerPort - 100
			return
		}
	}

	ctx := context.Background()
	client := dht.NewDhtServiceClient(connection)
	req := dht.Key{IsClient: false, Key: 0}

	for {
		fmt.Println(" ")
		log.Printf("Checking if servers are synced")
		_, err := client.Get(ctx, &req)
		if err != nil {
			log.Printf("Lost connection. Dialed to new server")
			if ser.PeerPort == 8800 {
				ser.PeerPort = 9000
				if ser.Port == ser.PeerPort {
					log.Fatalf("Only one server left, the application has crashed. ")
				}
				return
			} else {
				ser.PeerPort = ser.PeerPort - 100
				if ser.Port == ser.PeerPort {
					log.Fatalf("Only one server left, the application has crashed. ")
				}
				return
			}
		}
		log.Printf("The servers are in sync")
		fmt.Println(" ")
		time.Sleep(5 * time.Second)

	}
}
