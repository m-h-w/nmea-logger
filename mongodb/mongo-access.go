package mongodb

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

/* 	There is an open access setting in the network access settings in The Mongo Portal
0.0.0.0/0 but it turns off after 6 hours. Go and turn it on when working on the project

Update: this is now on permanently
*/

// Record the number of data points ready to write to mongo DB for each reading
// Mongo insertMany has a limit of 1000documents. The best compromise for speed and load on the system is 100-200
// https://stackoverflow.com/questions/36042967/mongoose-insertmany-limit

type DbWriteCache_t struct {
	Count int                         //number of documents waiting to be written
	Mem   *[DB_WRITE_THRESHOLD][]byte //pomter to array of []byte where the bson documents are cached before they are written
}

const DB_WRITE_THRESHOLD = 100 // the threshold at which the cache is written to Mongo.

var writeCache DbWriteCache_t // this structure manages the cache
var wg sync.WaitGroup         // waits for the threads to complete before closing Mongo connection.
var mongoClient *mongo.Client // the actual mong client object

// Debug vars
var debug bool = true
var start, done int

// see https://docs.mongodb.com/drivers/go/current/fundamentals/crud/write-operations/insert/
// and https://pkg.go.dev/go.mongodb.org/mongo-driver@v1.8.0/mongo#Collection.InsertMany

// write cache is called by reference so  the calling thread can set up a new cache while this thread
// uses the old cache and hopefully frees it.
func writeCacheToDB(localCache DbWriteCache_t, collection string) {

	defer wg.Done() // sync up all the threads before closing mongo DB connection.

	if debug {
		fmt.Printf("Writing to DB start %d\n", start)
		start++
	}
	activeDB := os.Getenv("ACTIVEDB")
	//collection := os.Getenv("COLLECTION")

	coll := mongoClient.Database(activeDB).Collection(collection)

	// Copy the cache into an []interface {} - Not 100% sure why this is necessary.
	// I cant coerce the compiler to cast the array of bson strings to an array of interface{}
	var docs []interface{} = make([]interface{}, localCache.Count)
	for i := 0; i < localCache.Count; i++ {
		docs[i] = localCache.Mem[i]
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

	if debug {
		fmt.Printf("Writing to DB done %d\n", done)
		done++
	}
}

func flushCache(collection string) {

	if writeCache.Count != 0 { // check if there are any unread data in the cache

		if debug {
			// Write cache to DB
			fmt.Printf("flushing cache to DB \n")
		}

		// write last data to DB
		wg.Add(1)
		go writeCacheToDB(writeCache, collection) //send a COPY of the global writeCache to writeCacheToDB

	} else {

		if debug {
			// Write cache to DB
			fmt.Printf("Cache was empty\n")
		}
	}

}

// Public Functions
// ----------------

// Reads all of though data points of a particular type. E.g magheading or boatspeed etc
func ReadAll(SearchElement string, collection string) *mongo.Cursor {
	if debug {
		fmt.Printf("ReadAll\n")
	}

	activeDB := os.Getenv("ACTIVEDB")
	coll := mongoClient.Database(activeDB).Collection(collection)

	// select all the documents that contain the searchElement we are searching for
	filter := bson.M{SearchElement: bson.M{"$exists": true}}

	cursor, err := coll.Find(context.TODO(), filter)

	if err != nil {
		fmt.Printf("crash in func ReadAll()\n")
		log.Fatal(err)
	}
	return cursor
}

/*
func ReadFromMongoBetweenTimes(startTime string, endtime string, searchItem string) {

	activeDB := os.Getenv("ACTIVEDB")

	coll := mongoClient.Database(activeDB).Collection(collection)
	cursor, err := coll.Find(context.TODO(),
		bson.D{{"ts": {"$gte": ISODate(startTime), "$lte": ISODate(endTime)}, searchItem: {"$exists": true}}})

	if err != nil {
		log.Fatal(err)
	}
	var results []bson.D
	if err = cursor.All(context.TODO(), &results); err != nil {
		log.Fatal(err)
	}
	for _, result := range results {
		fmt.Println(result)
	}

}
*/

// takes a []byte of bson values for all the document types and caches DB_WRITE_THRESHOLD documents before writing them to
// Mongo using insertMany

func WriteToMongo(v []byte, collection string) {

	if debug {
		fmt.Printf("Writing to cache %d \n", writeCache.Count)
	}

	if writeCache.Count == (DB_WRITE_THRESHOLD - 1) { // 0-99 not 1-100

		if debug {
			// Write cache to DB
			fmt.Printf("write cache to DB \n")
		}

		// write the last json doc to cache
		writeCache.Mem[writeCache.Count] = v
		writeCache.Count++ // the count should now be 100, the amout of data in the cache.

		// write to the DB in a separate thread
		// writeCache passed by value so that calling thread can set up a new cache.

		wg.Add(1)
		go writeCacheToDB(writeCache, collection) //send a COPY of the global writeCache data structure to writeCacheToDB

		// set up a new cache so the go routine can work on the old one
		writeCache.Mem = new([DB_WRITE_THRESHOLD][]byte)
		writeCache.Count = 0 //reset the write  counter for the next 100 documents

	} else {

		// write bson data to cache
		writeCache.Mem[writeCache.Count] = v
		// update the write count for this document. Write to DB when it reaches DB_WRITE_THRESHOLD (100)
		writeCache.Count++
	}

}

func InitMongoConnection() { // for the initial write to Mongo from the data logger

	// initialise the write cache - this is a naive implementation that expect only
	// one conection to exist at a time.
	writeCache.Mem = new([DB_WRITE_THRESHOLD][]byte)
	writeCache.Count = 0

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
	mongoClient = client

}

func ListCollections() []string {

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

	db := client.Database(os.Getenv("ACTIVEDB"))
	colls, err := db.ListCollectionNames(context.TODO(), bson.D{})

	if err != nil {
		panic(err)
	}
	if err := client.Disconnect(context.TODO()); err != nil {
		panic(err)
	}
	return colls
}

func CloseMongoConnection(collection string) { // disconnect following a write from the data logger

	flushCache(collection)
	if debug {
		fmt.Print("waiting for last thread to finish\n")
	}
	wg.Wait() // wait for all the threads to finish before closing the mongo connection.

	if err := mongoClient.Disconnect(context.TODO()); err != nil {
		panic(err)
	}

	fmt.Println("Connection to MongoDB closed.")

}
