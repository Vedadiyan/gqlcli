package main

import (
	"encoding/json"
	"os"

	flaggy "github.com/vedadiyan/flaggy/pkg"
	_ "github.com/vedadiyan/gql/pkg/functions/avg"
	_ "github.com/vedadiyan/gql/pkg/functions/common"
	_ "github.com/vedadiyan/gql/pkg/functions/concat"
	_ "github.com/vedadiyan/gql/pkg/functions/count"
	_ "github.com/vedadiyan/gql/pkg/functions/first"
	_ "github.com/vedadiyan/gql/pkg/functions/max"
	_ "github.com/vedadiyan/gql/pkg/functions/min"
	_ "github.com/vedadiyan/gql/pkg/functions/mongo"
	_ "github.com/vedadiyan/gql/pkg/functions/nullifempty"
	_ "github.com/vedadiyan/gql/pkg/functions/once"
	_ "github.com/vedadiyan/gql/pkg/functions/redis"
	_ "github.com/vedadiyan/gql/pkg/functions/selectkey"
	_ "github.com/vedadiyan/gql/pkg/functions/sum"
	_ "github.com/vedadiyan/gql/pkg/functions/toarray"
	_ "github.com/vedadiyan/gql/pkg/functions/tobytes"
	_ "github.com/vedadiyan/gql/pkg/functions/todouble"
	_ "github.com/vedadiyan/gql/pkg/functions/toint"
	_ "github.com/vedadiyan/gql/pkg/functions/tomap"
	_ "github.com/vedadiyan/gql/pkg/functions/tostring"
	_ "github.com/vedadiyan/gql/pkg/functions/unwind"
	_ "github.com/vedadiyan/gql/pkg/functions/uuid"
	_ "github.com/vedadiyan/gql/pkg/functions/valueof"
	gql "github.com/vedadiyan/gql/pkg/sql"
)

type Options struct {
	Query       string `long:"--query" short:"-q" help:"path to file containing the GQL query"`
	Source      string `long:"--src" short:"-s" help:"path to the data source"`
	Destination string `long:"--dest" short:"-d" help:"path to destination to save the result"`
	Help        bool   `long:"--help" short:"-h" help:"displays help"`
}

func (options Options) Run() error {
	if options.Help {
		flaggy.PrintHelp()
		return nil
	}
	src, err := os.ReadFile(options.Source)
	if err != nil {
		return err
	}
	mapper := make(map[string]any)
	err = json.Unmarshal(src, &mapper)
	if err != nil {
		return err
	}
	query, err := os.ReadFile(options.Query)
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
	err = os.WriteFile(options.Destination, jsonData, os.ModePerm)
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
