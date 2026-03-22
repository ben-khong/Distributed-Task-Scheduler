package main

import (
	"flag"
	"log"

	"github.com/ben-khong/Distributed-Task-Scheduler/pkg/common"
	"github.com/ben-khong/Distributed-Task-Scheduler/pkg/coordinator"
)

var (
	coordinatorPort = flag.String("coordinator_port", ":8080", "Port on which the Coordinator serves requests.")
)

func main() {
	flag.Parse()
	dbConnectionString := common.GetDBConnectionString()
	server := coordinator.NewServer(*coordinatorPort, dbConnectionString)
	if err := server.Start(); err != nil {
		log.Fatalf("Error while starting coordinator: %+v", err)
	}
}
