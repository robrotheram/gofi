package main

import (
	"context"
	"fmt"
	"github.com/olivere/elastic"
	"log"
)

func createClient() {
	var err error
	ESclient, err = elastic.NewSimpleClient(elastic.SetURL("http://192.168.0.125:9200"))

	if err != nil {
		log.Fatal(err)
	}
	if err != nil {
		log.Fatal(err)
	}
	Logger.Info("Successfully connected to Elastic: " + Settings.Elastic)
}

func sendToElastic(v ArticleStruct) {

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