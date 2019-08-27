package main

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gomodule/redigo/redis"
)

func main() {
	// Initialize a connection pool and assign it to
	// the pool global variable
	pool = &redis.Pool{
		MaxIdle:     10,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", ":6379")
		},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/album", showAlbum)
	log.Println("Listening on :4000...")
	http.ListenAndServe(":4000", mux)
}

func showAlbum(w http.ResponseWriter, r *http.Request) {
	// Unless the request is a GET method, return a
	// 405 'Method not alllowed' response
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, http.StatusText(405), 405)
		return
	}

	// Retrieve the id from the request URL query string.
	// If there is no id key in the query string then Get() will return
	// an empty string.
	id := r.URL.Query().Get("id")
	if id == "" {
		http.Error(w, http.StatusText(400), 400)
		return
	}
	bk, err := FindAlbum(id)
	if err == ErrNoAlbum {
		http.NotFound(w, r)
		return
	} else if err != nil {
		http.Error(w, http.StatusText(500), 500)
		return
	}
	// Write the album details as plain text to the client.
	fmt.Fprintf(w, "%s by %s: £%.2f [%d likes] \n", bk.Title, bk.Artist, bk.Price, bk.Likes)
}
