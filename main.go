package main

import (
	"time"

	backend "github.com/DCSO/balboa/backend/go"
	"github.com/DCSO/balboa/observation"
	"github.com/satta/balboa-backend-dgraph/handler"

	"github.com/dgraph-io/dgo"
	"github.com/dgraph-io/dgo/protos/api"
	"github.com/patrickmn/go-cache"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

func main() {
	dconn, err := grpc.Dial("localhost:9080", grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	defer dconn.Close()
	dgraphClient := dgo.NewDgraphClient(api.NewDgraphClient(dconn))

	h := &handler.DgraphHandler{
		Client:        dgraphClient,
		BufChunks:     make([]*observation.InputObservation, 0),
		SensorIDCache: cache.New(30*time.Minute, 45*time.Minute),
	}
	h.CheckAndSetSchema()
	backend.Serve("0.0.0.0:4242", h)
	log.Info("shutdown complete")
}
