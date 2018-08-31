package main

import (
	"context"
	"fmt"
	"github.com/olivere/elastic"
	"log"
)

func createClient() {
	var err error
	ESclient, err = elastic.NewSimpleClient(elastic.SetURL(Settings.Elastic))
	if err != nil {
		log.Fatal(err)
	}
	if err != nil {
		log.Fatal(err)
	}
	Logger.Info("Successfully connected to Elastic: " + Settings.Elastic)
}

func sendToElastic(v ArticleStruct) {
	return
	ctx := context.Background()

	bulkRequest := ESclient.Bulk()

	req := elastic.NewBulkIndexRequest().Index("article").Type("article").Id(GetMD5Hash(v.Url)).Doc(v)
	bulkRequest = bulkRequest.Add(req)

	bulkResponse, err := bulkRequest.Do(ctx)
	if err != nil {
		fmt.Println(err)
	}
	if bulkResponse != nil {

	}
}
