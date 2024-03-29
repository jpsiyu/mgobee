package mgobee

import (
	"context"
	"errors"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type Bee struct {
	client                     *mongo.Client
	dbName, dbUser, dbPassword string
	dbUrls                     []string
}

func Create(dbName, dbUser, dbPassword string, dbUrls []string) *Bee {
	bee := Bee{
		dbName:     dbName,
		dbUser:     dbUser,
		dbPassword: dbPassword,
		dbUrls:     dbUrls,
	}
	return &bee
}

func (bee *Bee) GetReplaceOptions() *options.ReplaceOptions {
	return options.Replace()
}

func (bee *Bee) GetFindOptions() *options.FindOptions {
	return options.Find()
}

func (bee *Bee) Connect(url string) error {
	client, err := bee.creatConnectedClient(url)
	if err != nil {
		return err
	}
	err = bee.pingDB(client)
	if err != nil {
		return err
	}
	bee.client = client
	return nil
}

func (bee *Bee) SmartConnect() error {
	var url string
	num := len(bee.dbUrls)
	type dbChanData struct {
		client *mongo.Client
		err    error
	}
	dbChan := make(chan dbChanData, num)
	for i := 0; i < num; i++ {
		url = bee.dbUrls[i]
		go func(u string) {
			client, err := bee.creatConnectedClient(u)
			if err != nil {
				dbChan <- dbChanData{client: client, err: err}
				return
			}
			err = bee.pingRepeat(client, 10)
			dbChan <- dbChanData{client: client, err: err}
		}(url)
	}
	for i := 0; i < num; i++ {
		chanRes := <-dbChan
		if chanRes.err == nil {
			bee.client = chanRes.client
			defer close(dbChan)
			return nil
		}
	}
	return errors.New("No alive connection")
}

func (bee *Bee) Ping() error {
	return bee.pingDB(bee.client)
}

func (bee *Bee) Insert(document interface{}, collectionName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	collection := bee.client.Database(bee.dbName).Collection(collectionName)
	_, err := collection.InsertOne(ctx, document)
	return err
}

func (bee *Bee) InsertMany(document []interface{}, collectionName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	collection := bee.client.Database(bee.dbName).Collection(collectionName)
	_, err := collection.InsertMany(ctx, document)
	return err
}

func (bee *Bee) Update(filter, update interface{}, collectionName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	collection := bee.client.Database(bee.dbName).Collection(collectionName)
	_, err := collection.UpdateOne(ctx, filter, update)
	return err
}

func (bee *Bee) Find(filter interface{}, collectionName string, opt ...*options.FindOptions) ([]interface{}, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	collection := bee.client.Database(bee.dbName).Collection(collectionName)
	cur, err := collection.Find(ctx, filter, opt...)
	defer cur.Close(context.Background())
	if err != nil {
		return nil, err
	}
	results := []interface{}{}
	for cur.Next(ctx) {
		var result map[string]interface{}
		err := cur.Decode(&result)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	return results, cur.Err()
}

func (bee *Bee) Delete(filter interface{}, collectionName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	collection := bee.client.Database(bee.dbName).Collection(collectionName)
	_, err := collection.DeleteOne(ctx, filter)
	return err
}

func (bee *Bee) Replace(filter, replacement interface{}, collectionName string, opt *options.ReplaceOptions) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	collection := bee.client.Database(bee.dbName).Collection(collectionName)
	_, err := collection.ReplaceOne(ctx, filter, replacement, opt)
	return err
}

func (bee *Bee) pingDB(client *mongo.Client) error {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	err := client.Ping(ctx, readpref.Primary())
	return err
}

func (bee *Bee) pingRepeat(client *mongo.Client, count int) error {
	var err error
	for i := 0; i < count; i++ {
		err = bee.pingDB(client)
		if err == nil {
			return nil
		} else {
			time.Sleep(time.Second)
		}
	}
	return errors.New("Ping failed")
}

func (bee *Bee) creatConnectedClient(url string) (*mongo.Client, error) {
	cdt := options.Credential{Username: bee.dbUser, Password: bee.dbPassword}
	client, err := mongo.NewClient(options.Client().ApplyURI(url).SetAuth(cdt))
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = client.Connect(ctx)
	return client, err
}
