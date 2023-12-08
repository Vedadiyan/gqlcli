package main

import (
	"context"
	"encoding/json"
	"os"

	"github.com/redis/go-redis/v9"
	flaggy "github.com/vedadiyan/flaggy/pkg"
	"github.com/vedadiyan/goal/pkg/di"
	gqlmongo "github.com/vedadiyan/gql/pkg/functions/mongo"
	gqlredis "github.com/vedadiyan/gql/pkg/functions/redis"
	gql "github.com/vedadiyan/gql/pkg/sql"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"gopkg.in/yaml.v3"
)

type Options struct {
	Query          string  `long:"--query" short:"-q" help:"path to file containing the GQL query"`
	Source         string  `long:"--src" short:"-s" help:"path to the data source"`
	Destination    string  `long:"--dest" short:"-d" help:"path to destination to save the result"`
	Configurations *string `long:"--conf" short:"-c" help:"additional configurations"`
	Help           bool    `long:"--help" short:"-h" help:"displays help"`
}

func (opts Options) Run() error {
	if opts.Help {
		flaggy.PrintHelp()
		return nil
	}
	if opts.Configurations != nil {
		bytes, err := os.ReadFile(*opts.Configurations)
		if err != nil {
			return err
		}
		conf := make(map[string]map[string]string)
		err = yaml.Unmarshal(bytes, &conf)
		if err != nil {
			return err
		}
		redisConf, ok := conf["redis"]
		if ok {
			for key, value := range redisConf {
				client := redis.NewClient(&redis.Options{
					Addr: value,
				})
				di.AddSinletonWithName[*redis.Client](key, func() (instance *redis.Client, err error) {
					return client, nil
				})
			}
			gqlredis.RegisterConManager(func(connKey string) (*redis.Client, error) {
				redisClient, err := di.ResolveWithName[*redis.Client](connKey, nil)
				if err != nil {
					return nil, err
				}
				return *redisClient, nil
			})
		}
		mongoConf, ok := conf["mongo"]
		if ok {
			for key, value := range mongoConf {
				client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(value))
				if err != nil {
					panic(err)
				}
				di.AddSinletonWithName[*mongo.Client](key, func() (instance *mongo.Client, err error) {
					return client, nil
				})
			}
			gqlmongo.RegisterConManager(func(connKey string) (*mongo.Client, error) {
				mongoClient, err := di.ResolveWithName[*mongo.Client](connKey, nil)
				if err != nil {
					return nil, err
				}
				return *mongoClient, nil
			})
		}
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
