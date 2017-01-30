package main

import (
    "context"
    "fmt"
    "math"
    "os"
    "time"

    "github.com/pborman/uuid"
    "gopkg.in/btrdb.v4"
)

const NUM_POINTS = 100

func main() {
    if len(os.Args) != 3 {
        fmt.Printf("Usage: %s <collection> <UUID>\n", os.Args[0])
        os.Exit(1)
    }

    ctx := context.Background()

    btrdbconn, err := btrdb.Connect(ctx, btrdb.EndpointsFromEnv()...)
    if err != nil {
        panic(err)
    }

    u := uuid.Parse(os.Args[2])
    stream := btrdbconn.StreamFromUUID(u)
    exists, err := stream.Exists(ctx)
    if err != nil {
        panic(err)
    }

    now := time.Now().UnixNano()

    if !exists {
        stream, err = btrdbconn.Create(ctx, u, os.Args[1], map[string]string{"timecreated": fmt.Sprintf("%v", now)}, []byte{})
        if err != nil {
            panic(err)
        }
    }

    points := make([]btrdb.RawPoint, NUM_POINTS, NUM_POINTS)

    for i := 0; i != len(points); i++ {
        points[i].Time = now + int64(1000000000) * int64(i)
        points[i].Value = math.Sin(float64(i) / float64(10))
    }

    err = stream.Insert(ctx, points)
    if err != nil {
        panic(err)
    }

    err = btrdbconn.Disconnect()
    if err != nil {
        panic(err)
    }

    fmt.Println("Success")
}
