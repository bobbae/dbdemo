package main

import (
	"context"
	"fmt"
	"log"
	"flag"

	"github.com/bobbae/q"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/bson"
	//"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

const uri = "mongodb://root:rootpass@localhost:27017/?maxPoolSize=20&w=majority"


func main() {
	flag.Parse()

	fmt.Println("Connecting to", uri)
	// Create a new client and connect to the server
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(uri))
	if err != nil {
		panic(err)
	}
	/* defer func() {
		if err = client.Disconnect(context.TODO()); err != nil {
			panic(err)
		}
	}() */

	if err := client.Ping(context.TODO(), readpref.Primary()); err != nil {
		panic(err)
	}
	fmt.Println("Successfully connected and pinged.")

	collection := client.Database("recordings").Collection("albums")
	albums := []interface{}{
		bson.D{{"_id", 1}, {"title", "Blue Train"}, {"artist", "John Coltrane"}, {"price", 56.99}},
  		bson.D{{"_id", 2}, {"title", "Giant Steps"}, {"artist", "John Coltrane"}, {"price", 63.99}},
  		bson.D{{"_id", 3}, {"title", "Jeru"}, {"artist", "Gerry Mulligan"}, {"price", 17.99}},
  		bson.D{{"_id", 4}, {"title", "Sarah Vaughan"}, {"artist", "Sarah Vaughan"}, {"price", 34.98}},
	}

	//TODO id value can be a hash of title and artist to avoid duplicate title-artist pairs
	//   Or use compound index.
	
	insertResult, err := collection.InsertOne(context.TODO(), albums[0])
	if err != nil {
		log.Println(err)
	} else {
		fmt.Println("Inserted a single document: ", insertResult.InsertedID)
	}

	insertManyResult, err := collection.InsertMany(context.TODO(), albums[1:])

	if err != nil {
		log.Println(err)
	} else {
		fmt.Println("Inserted multiple documents: ", insertManyResult.InsertedIDs)
	}


	filter := bson.D{{"artist", "John Coltrane"}}
	update := bson.D{
		{ "$inc", bson.D{
			{"price", 1},
		}},
	}

	updateResult, err := collection.UpdateOne(context.TODO(), filter, update)
	if err != nil {
		log.Fatal(err)
	} 
	fmt.Printf("Matched %v documents and updated %v documents.\n", updateResult.MatchedCount, updateResult.ModifiedCount)

	var result bson.M
	err = collection.FindOne(context.TODO(), filter).Decode(&result)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Found a document: %+v\n", result)

	findOptions := options.Find()
	findOptions.SetLimit(20)

	var results []bson.M

	cur, err := collection.Find(context.TODO(), bson.D{{}}, findOptions)
	if err != nil {
		log.Fatal(err)
	}

	for cur.Next(context.TODO()) {
		var elem bson.M
		err := cur.Decode(&elem)
		if err != nil {
			log.Fatal(err)
		}

		q.Q(elem)
		results = append(results, elem)
	}
	
	if err := cur.Err(); err != nil {
		log.Fatal(err)
	}
	cur.Close(context.TODO())

	fmt.Printf("Found multiple documents: %+v\n", results)
	err = client.Disconnect(context.TODO())
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Disconnected.")
}
