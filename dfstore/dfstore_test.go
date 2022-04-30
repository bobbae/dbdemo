package dfstore_test

import (
	"context"
	"log"

	"testing"

	"dfstore"
	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
)

func TestExample1(t *testing.T) {
	dataRows := [][]string{
		{"title", "artist", "price"},
		{"Blue Train", "John Coltrane", "56.99"},
		{"Giant Steps", "John Coltrane", "63.99"},
		{"Jeru", "Gerry Mulligan", "17.99"},
		{"Sarah Vaughan", "Sarah Vaughan", "34.98"},
	}

	dfs, err := dfstore.New(context.TODO(), "default")
	if err != nil {
		t.Errorf("cannot get new dfstore, %v",err)
		return
	}
	defer dfs.Close()

	err = dfs.WriteRecords(dataRows)
	if err != nil {
		t.Errorf("cannot write, %v", err)
	}
	filters := []dataframe.F{
		dataframe.F{Colname: "artist", Comparator: series.Eq, Comparando: "John Coltrane"},
		dataframe.F{Colname: "price", Comparator: series.Greater, Comparando: "50"},
	}
	res, err := dfs.ReadRecords(filters, 20)
	if err != nil {
		t.Errorf("cannot read, %v", err)
	}
	log.Println("read", res)
}
