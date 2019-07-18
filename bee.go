package mgobee

import (
	"context"
	"errors"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type Bee struct {
	client                     *mongo.Client
	dbName, dbUser, dbPassword string
	dbUrls                     []string
}

func Create(dbName, dbUser, dbPassword string, dbUrls []string) Bee {
	bee := Bee{
		dbName:     dbName,
		dbUser:     dbUser,
		dbPassword: dbPassword,
		dbUrls:     dbUrls,
	}
	return bee
}

func (bee *Bee) creatConnectedClient(url string) (*mongo.Client, error){
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

func (bee *Bee) isClientConnecting(client *mongo.Client) error {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	err := client.Ping(ctx, readpref.Primary())	
	return err
} 

func (bee *Bee) Connect(url string) error {
	client, err := bee.creatConnectedClient(url)
	if err != nil{
		return err
	}
	err = bee.isClientConnecting(client)
	if err != nil{
		return err
	}
	bee.client = client
	return nil
}

func (bee *Bee) SmartConnect(c chan error) {
	var err error
	for i := 0; i < len(bee.dbUrls); i++ {
		log.Println("connect to db url", bee.dbUrls[i])
		err = bee.Connect(bee.dbUrls[i])
		if err != nil {
			c <- err
			return
		}
		for j := 0; j < 10; j++ {
			err = bee.Ping()
			if err != nil {
				log.Println("ping fail", j)
				time.Sleep(1 * time.Second)
			} else {
				log.Println("ping success")
				c <- nil
				return
			}
		}
	}
	c <- errors.New("connect failed")
}

func (bee *Bee) Ping() error {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	return bee.client.Ping(ctx, readpref.Primary())
}

func (bee *Bee) Insert(document interface{}, collectionName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	collection := bee.client.Database(bee.dbName).Collection(collectionName)
	_, err := collection.InsertOne(ctx, document)
	return err
}

func (bee *Bee) Update(filter, update *bson.M, collectionName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	collection := bee.client.Database(bee.dbName).Collection(collectionName)
	_, err := collection.UpdateOne(ctx, *filter, *update)
	return err
}

func (bee *Bee) Find(results *[]bson.M, filter *bson.M, collectionName string, opt ...*options.FindOptions) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	collection := bee.client.Database(bee.dbName).Collection(collectionName)
	cur, err := collection.Find(ctx, *filter, opt...)
	defer cur.Close(context.Background())
	if err != nil {
		return err
	}
	for cur.Next(ctx) {
		var result bson.M
		err := cur.Decode(&result)
		if err != nil {
			return err
		}
		*results = append(*results, result)
	}
	return cur.Err()
}

func (bee *Bee) Delete(filter *bson.M, collectionName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	collection := bee.client.Database(bee.dbName).Collection(collectionName)
	_, err := collection.DeleteOne(ctx, filter)
	return err
}
