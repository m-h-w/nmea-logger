package mongodb

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var debug bool = true

// Record the number of data points ready to write to mongo DB for each reading
// Mongo insertMany has a limit of 1000documents. The best compromise for speed and load on the system is 100-200
// https://stackoverflow.com/questions/36042967/mongoose-insertmany-limit

const DB_WRITE_THRESHOLD = 100

type DbWriteCache_t struct {
	Count int                         //number of documents waiting to be written
	Mem   *[DB_WRITE_THRESHOLD][]byte //pomter to array of []byte where the bson documents are cached before they are written
}

func InitMongoConnection() *mongo.Client {

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

	return client
}

func CloseMongoConnection(client *mongo.Client) {

	if err := client.Disconnect(context.TODO()); err != nil {
		panic(err)
	}

}

/* 	There is an open access setting in the network access settings in The Mongo Portal
0.0.0.0/0 but it turns off after 6 hours. Go and turn it on when working on the project
*/

// see https://docs.mongodb.com/drivers/go/current/fundamentals/crud/write-operations/insert/
// and https://pkg.go.dev/go.mongodb.org/mongo-driver@v1.8.0/mongo#Collection.InsertMany

func WriteCacheToDB(client *mongo.Client, writeCache *DbWriteCache_t) {

	activeDB := "fe28timeseries" //os.Getenv("ACTIVEDB")
	collection := "cogitalTest"  //os.Getenv("COLLECTION")

	coll := client.Database(activeDB).Collection(collection)

	// Copy the cache into an []interface {} - Not 100% sure why this is necessary.
	// I cant coerce the compiler to cast the array of bson strings to an array of interface{}
	var docs []interface{} = make([]interface{}, writeCache.Count)
	for i := 0; i < writeCache.Count; i++ {
		docs[i] = writeCache.Mem[i]
	}

	result, err := coll.InsertMany(context.TODO(), docs)

	if err != nil {
		fmt.Printf("ActiveDB = %s\nCollection = %s\n", activeDB, collection)
		panic(err)
	}
	// stop the compiler moaning that I am not using result
	if result == nil {
		os.Exit(0)
	}
	//test
	/*
		list_ids := result.InsertedIDs
		fmt.Printf("Documents inserted: %v\n", len(list_ids))
		for _, id := range list_ids {
			fmt.Printf("Inserted document with _id: %v\n", id)
		}

		os.Exit(0)
	*/
}
