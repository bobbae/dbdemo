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
	json, err := json.Marshal(Album{Title: "The Modern Sound of Betty Carter", Artist: "Betty Carter", Price: 25.99})
	if err != nil {
		fmt.Println(err)
	}

	err = client.Set("id1234", json, 0).Err()
	if err != nil {
		fmt.Println(err)
	}
	val, err := client.Get("id1234").Result()
	if err != nil {
		fmt.Println(err)
	}
	q.Q("Got:", val)

	albums := []map[string]interface{}{
		{"title": "Blue Train", "artist": "John Coltrane", "price": 56.99},
		{"title": "Giant Steps", "artist": "John Coltrane", "price": 63.99},
		{"title": "Jeru", "artist": "Gerry Mulligan", "price": 17.99},
		{"title": "Sarah Vaughan", "artist": "Sarah Vaughan", "price": 34.98},
	}
	q.Q(albums)

	var key string
	for i, album := range albums { 
		key = fmt.Sprintf("album:%d",i)
		q.Q(key, album)
		_,err = client.HMSet(key, album).Result()
		if err != nil {
			fmt.Println(err)
		}
	}
	hval, err := client.HMGet("album:2","title", "price").Result()
	if err != nil {
		fmt.Println(err)
	}
	q.Q("Got:", hval)
	hkeys := client.HKeys("album:2")
	q.Q("HKEYS",hkeys)

	keys := client.Keys("album:*")
	q.Q("Keys",keys)
}