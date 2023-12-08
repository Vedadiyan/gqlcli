package main

import (
	"context"
	"encoding/json"
	"os"

	"github.com/redis/go-redis/v9"
	flaggy "github.com/vedadiyan/flaggy/pkg"
	gqlmongo "github.com/vedadiyan/gql/pkg/functions/mongo"
	gqlredis "github.com/vedadiyan/gql/pkg/functions/redis"
	gql "github.com/vedadiyan/gql/pkg/sql"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Options struct {
	Query       string  `long:"--query" short:"-q" help:"path to file containing the GQL query"`
	Source      string  `long:"--src" short:"-s" help:"path to the data source"`
	Destination string  `long:"--dest" short:"-d" help:"path to destination to save the result"`
	Redis       *string `long:"--redis" short:"-r" help:"redis connection string"`
	Mongo       *string `long:"--mongo" short:"-m" help:"mongodb connection string"`
	Help        bool    `long:"--help" short:"-h" help:"displays help"`
}

func (opts Options) Run() error {
	if opts.Help {
		flaggy.PrintHelp()
		return nil
	}
	if opts.Redis != nil {
		client := redis.NewClient(&redis.Options{
			Addr: *opts.Redis,
		})
		gqlredis.RegisterConManager(func(connKey string) (*redis.Client, error) {
			return client, nil
		})
	}
	if opts.Mongo != nil {
		client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(*opts.Mongo))
		if err != nil {
			panic(err)
		}
		gqlmongo.RegisterConManager(func(connKey string) (*mongo.Client, error) {
			return client, nil
		})
	}
	src, err := os.ReadFile(opts.Source)
	if err != nil {
		return err
	}
	mapper := make(map[string]any)
	err = json.Unmarshal(src, &mapper)
	if err != nil {
		return err
	}
	query, err := os.ReadFile(opts.Query)
	if err != nil {
		return err
	}
	gqlContext := gql.New(mapper)
	err = gqlContext.Prepare(string(query))
	if err != nil {
		return err
	}
	rs, err := gqlContext.Exec()
	if err != nil {
		return err
	}
	jsonData, err := json.MarshalIndent(rs, "", "\t")
	if err != nil {
		return err
	}
	err = os.WriteFile(opts.Destination, jsonData, os.ModePerm)
	if err != nil {
		return err
	}
	return nil
}

func main() {
	options := Options{}
	err := flaggy.Parse(&options, os.Args[1:])
	if err != nil {
		panic(err)
	}
}
