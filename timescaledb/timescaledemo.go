package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/bobbae/q"
	pq "github.com/lib/pq"
)

var db *sql.DB

type Album struct {
	ID     int64
	Title  string
	Artist string
	Price  float32
}

const (
	host     = "localhost"
	port     = 5432
	user     = "postgres"
	password = "password"
	dbname   = "testdb"
)

func main() {
	flag.Parse()
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
	var err error
	db, err = sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	pingErr := db.Ping()
	if pingErr != nil {
		log.Fatal(pingErr)
	}
	q.Q("connected")

	//db.Query("CREATE database recordings")
	//db.Query("USE recordings")

	db.Query("DROP TABLE IF EXISTS albums")

	db.Query(`
	CREATE TABLE album (
		id         SERIAL PRIMARY KEY,
		title      VARCHAR(128) NOT NULL,
		artist     VARCHAR(255) NOT NULL,
		price      DECIMAL(5,2) NOT NULL
	  )
	`)

	db.Query("ALTER TABLE album ADD CONSTRAINT title_artist UNIQUE(title,artist)")

	db.Query(`
	INSERT INTO album
  		(title, artist, price)
		VALUES
  		('Blue Train', 'John Coltrane', 56.99),
  		('Giant Steps', 'John Coltrane', 63.99),
  		('Jeru', 'Gerry Mulligan', 17.99),
  		('Sarah Vaughan', 'Sarah Vaughan', 34.98);
	`)
	albums, err := albumsByArtist("John Coltrane")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Albums found: %v\n", albums)

	alb, err := albumByID(2)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Album found: %v\n", alb)

	albID, err := addAlbum(Album{
		Title:  "The Modern Sound of Betty Carter",
		Artist: "Betty Carter",
		Price:  49.99,
	})
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("ID of added album: %v\n", albID)

	//https://github.com/lib/pq/blob/master/example/listen/doc.go
	reportProblem := func(ev pq.ListenerEventType, err error) {
		if err != nil {
			fmt.Println(err.Error())
		}
	}
	minReconn := 10 * time.Second
	maxReconn := time.Minute
	listener := pq.NewListener(psqlInfo, minReconn, maxReconn, reportProblem)
	err = listener.Listen("getwork")
	if err != nil {
		panic(err)
	}
	fmt.Println("entering main loop")
	for {
		// process all available work before waiting for notifications
		getWork(db)
		waitForNotification(listener)
	}
}

func albumsByArtist(name string) ([]Album, error) {
	// An albums slice to hold data from returned rows.
	var albums []Album

	rows, err := db.Query("SELECT * FROM album WHERE artist = $1", name)
	if err != nil {
		return nil, fmt.Errorf("albumsByArtist %q: %v", name, err)
	}
	defer rows.Close()
	// Loop through rows, using Scan to assign column data to struct fields.
	for rows.Next() {
		var alb Album
		if err := rows.Scan(&alb.ID, &alb.Title, &alb.Artist, &alb.Price); err != nil {
			return nil, fmt.Errorf("albumsByArtist scan %q: %v", name, err)
		}
		albums = append(albums, alb)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("albumsByArtist  rows error %q: %v", name, err)
	}
	return albums, nil
}

func albumByID(id int64) (Album, error) {
	var alb Album

	row := db.QueryRow("SELECT * FROM album WHERE id = $1", id)
	if err := row.Scan(&alb.ID, &alb.Title, &alb.Artist, &alb.Price); err != nil {
		if err == sql.ErrNoRows {
			return alb, fmt.Errorf("albumsById %d: no such album", id)
		}
		return alb, fmt.Errorf("albumsById %d: %v", id, err)
	}
	return alb, nil
}

func addAlbum(alb Album) (int64, error) {
	var id int64
	err := db.QueryRow("INSERT INTO album (title, artist, price) VALUES ($1, $2, $3) RETURNING id",alb.Title, alb.Artist, alb.Price).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("addAlbum: %v", err)
	}

	return id, nil
}

func doWork(db *sql.DB, work int64) {
	// work here
}
func getWork(db *sql.DB) {
	for {
		// get work from the database here
		var work sql.NullInt64
		err := db.QueryRow("SELECT get_work()").Scan(&work)
		if err != nil {
			fmt.Println("call to get_work() failed: ", err)
			time.Sleep(10 * time.Second)
			continue
		}
		if !work.Valid {
			// no more work to do
			fmt.Println("ran out of work")
			return
		}
		fmt.Println("starting work on ", work.Int64)
		go doWork(db, work.Int64)
	}
}
func waitForNotification(l *pq.Listener) {
	select {
		case <-l.Notify:
			fmt.Println("received notification, new work available")
		case <-time.After(90 * time.Second):
			go l.Ping()
			// Check if there's more work available, just in case it takes
			// a while for the Listener to notice connection loss and
			// reconnect.
			fmt.Println("received no work for 90 seconds, checking for new work")
	}
}