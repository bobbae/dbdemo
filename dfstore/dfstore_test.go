package dfstore_test

import (
	"context"
	"log"

	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
)

func example_test() {
	dataRows := [][]string{
		{"title", "artist", "price"},
		{"Blue Train", "John Coltrane", "56.99"},
		{"Giant Steps", "John Coltrane", "63.99"},
		{"Jeru", "Gerry Mulligan", "17.99"},
		{"Sarah Vaughan", "Sarah Vaughan", "34.98"},
	}

	dfs, err := dfstore.New("postgres://postgres:password@localhost:5432/testdb/albums?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer dfs.Close()

	err = dfs.WriteRecords(context.TODO(), dataRows)
	if err != nil {
		log.Fatal(err)
	}
	filters := []dataframe.F{
		dataframe.F{Colname: "artist", Comparator: series.Eq, Comparando: "John Coltrane"},
		dataframe.F{Colname: "price", Comparator: series.Greater, Comparando: "50"},
	}
	res, err := dfs.ReadRecords(context.TODO(), filters)
	if err != nil {
		log.Fatal(err)
	}
	log.Println(res)

}
