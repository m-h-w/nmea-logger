package apimongo

import (
	"context"
	"encoding/json"
	"log"
	"os"

	"github.com/m-h-w/nmea-logger/transform"

	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var debug bool = true
var mongoClient *mongo.Client // the actual mongo client object

// very basic query with no paging (NP). Returns a mongo cursor containg the results
// query element <qe> in the collection <table>

func npBasicQuery(qe string, table string) *mongo.Cursor {

	activeDB := os.Getenv("ACTIVEDB")
	coll := mongoClient.Database(activeDB).Collection(table)

	// select all the documents that contain the searchElement we are searching for
	filter := bson.M{qe: bson.M{"$exists": true}}

	if debug {
		log.Printf("Querying table:%s in DB:%s", table, activeDB)
	}

	cursor, err := coll.Find(context.TODO(), filter)
	if err != nil {
		if debug {
			log.Printf("Error in NpBasicQuery coll.Find()")
		}
		log.Fatal(err)
	}

	return cursor

}

func InitDB() {

	log.Println("Connecting to Mongo DB.")

	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found")
	}
	uri := os.Getenv("MONGODB_URI")
	if uri == "" {
		log.Fatal("You must set your 'MONGODB_URI' environmental variable. See\n\t https://docs.mongodb.com/drivers/go/current/usage-examples/#environment-variable")
	}
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(uri))
	if err != nil {
		panic(err)
	}

	log.Println("Connected to Mongo DB.")
	mongoClient = client

}

func ShutDownDB() {

	if err := mongoClient.Disconnect(context.TODO()); err != nil {
		panic(err)
	}
	log.Println("Connection to MongoDB closed.")
}

func GetLrPosition(table string) ([]byte, error) {
	var results []transform.PositionData_t

	cursor := npBasicQuery("lat", table)

	if err := cursor.All(context.TODO(), &results); err != nil {
		log.Printf("error in NpBasicQuery")
		panic(err)
	}

	jsonResults, err := json.Marshal(results)

	if err != nil {
		log.Printf("Marshalling error in GetLRPosition() %s\n", err)
		return nil, err
	}

	return jsonResults, nil
}
