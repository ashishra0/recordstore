package main

import (
	"errors"

	"github.com/gomodule/redigo/redis"
)

// Declare a pool variable to hold the pool of Redis connections.
var pool *redis.Pool

var ErrNoAlbum = errors.New("no album found")

// Define a custom struct to hold Album data.
type Album struct {
	Title  string  `redis:"title"`
	Artist string  `redis:"artist"`
	Price  float64 `redis:"price"`
	Likes  int     `redis:"likes"`
}

func FindAlbum(id string) (*Album, error) {
	conn := pool.Get()
	defer conn.Close()

	// Fetch the details of a specific album. If no album is found
	// the given id, the []interface{} slice returned by redis.Values
	// will have a length of zero.
	values, err := redis.Values(conn.Do("HGETALL", "album:"+id))
	if err != nil {
		return nil, err
	} else if len(values) == 0 {
		return nil, ErrNoAlbum
	}

	var album Album
	err = redis.ScanStruct(values, &album)
	if err != nil {
		return nil, err
	}
	return &album, nil
}

func IncrementLikes(id string) error {
	conn := pool.Get()
	defer conn.Close()

	// Check if the album with the given id exists.
	exists, err := redis.Int(conn.Do("EXISTS", "album:"+id))
	if err != nil {
		return err
	} else if exists == 0 {
		return ErrNoAlbum
	}
	// Use the mutli command to inform Redis that we are starting a new
	// transaction. The conn.Send() method writes the command to the
	// connection's output buffer. It doesn't actually send it to the redis
	// connection
	err = conn.Send("MULTI")
	if err != nil {
		return nil
	}

	// Increment the number of likes in the album hash by 1. Because it
	// follows a MULTI command, the HINCRBY command wont be executed
	// but it is queued as part of the transaction.
	err = conn.Send("HINCRBY", "album:"+id, "likes", 1)
	if err != nil {
		return err
	}
	err = conn.Send("ZINCRBY", "likes", 1, id)
	if err != nil {
		return err
	}
	// Execute both commands in our transaction together as an atomic
	// group. EXEC returns the replies from both commands, but we are
	// not interested in replies, we just need to check for the errors
	_, err = conn.Do("EXEC")
	if err != nil {
		return err
	}
	return nil
}
