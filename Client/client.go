package main

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"

	dht "distributed-systems-exam/dht"
)

var port int32

func main() {
	port = 9000
	for {
		StartListen()
	}
}

func StartListen() {
	var random int = 0
	var key int32 = 0
	var value int32 = 0

	connection, err := grpc.Dial(toAddr(port), grpc.WithInsecure())
	defer connection.Close()

	if err != nil {
		if port == 8800 {
			port = 9000
			return
		} else {
			port = port - 100
			return
		}
	}

	ctx := context.Background()

	client := dht.NewDhtServiceClient(connection)

	random = rand.Intn(3)
	key = rand.Int31n(100)
	value = rand.Int31n(100)

	if random == 1 {
		log.Printf("Reqesting put with key: %d and value: %d \n", key, value)
		re := dht.Pair{IsClient: true, Key: key, Value: value}

		boolean, err := client.Put(ctx, &re)

		if err != nil {
			if port == 8800 {
				port = 9000
				return
			} else {
				port = port - 100
				return
			}
		}

		if boolean.WasSuccess {
			log.Println("The put operation was successful")
			fmt.Println(" ")
		} else {
			log.Println("Error - the put operation was NOT successsful")
			fmt.Println(" ")
		}

	} else {
		log.Printf("Requesting get with key: %d", key)
		req := dht.Key{IsClient: true, Key: key}
		returnedValue, err := client.Get(ctx, &req)

		if err != nil {
			if port == 8800 {
				port = 9000
				return
			} else {
				port = port - 100
				return
			}
		}

		log.Printf("The value for key %d is: %d", req.Key, returnedValue.Value)
		fmt.Println(" ")
	}

	time.Sleep(time.Duration(rand.Intn(10000)) * time.Millisecond)
}

func toAddr(port int32) string {
	return fmt.Sprintf(":%v", port)
}
