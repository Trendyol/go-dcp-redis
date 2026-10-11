package main

import (
	dcpredis "github.com/Trendyol/go-dcp-redis"
)

func main() {
	connector, err := dcpredis.NewConnectorBuilder("config.yml").
		Build()
	if err != nil {
		panic(err)
	}

	defer connector.Close()
	connector.Start()
}
