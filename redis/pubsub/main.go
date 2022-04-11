package main

import (
	//"context"
	"fmt"
	"log"
	"flag"
	"encoding/json"

	"github.com/bobbae/q"
	"github.com/go-redis/redis"
)

type Album struct {
	Title   string `json:"title"`
	Artist  string `json:"artist"`
	Price   float64 `json:"price"`
}
func main() {
	topic := flag.String("t", "test-topic", "topic")
	publish := flag.Bool("p", false, "publish")
	subscribe := flag.Bool("s", false, "subscribe")

	flag.Parse()
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	if err := client.Ping().Err(); err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	if *subscribe {
		q.Q("subscribing to", *topic)
		pubsub := client.Subscribe(*topic)
		defer pubsub.Close()
		for {
			msg, err := pubsub.ReceiveMessage()
			if err != nil {
				log.Fatal(err)
			}
			q.Q(msg.Channel, msg.Payload)
		}
	}
	if *publish {
		q.Q("publishing to", *topic)
		json, err := json.Marshal(Album{Title: "The Modern Sound of Betty Carter", Artist: "Betty Carter", Price: 25.99})
		if err != nil {
			fmt.Println(err)
		}

		err = client.Publish(*topic, json).Err()
		if err != nil {
			fmt.Println(err)
		}
	}
}