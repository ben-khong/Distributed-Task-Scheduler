package main

import (
	"flag"
	"log"

	"github.com/ben-khong/Distributed-Task-Scheduler/pkg/common"
	"github.com/ben-khong/Distributed-Task-Scheduler/pkg/scheduler"
)

var (
	schedulerPort = flag.String("scheduler_port", ":8081", "Port on which the Scheduler serves requests.")
)

func main() {
	flag.Parse()
	dbConnectionString := common.GetDBConnectionString()
	server := scheduler.NewServer(*schedulerPort, dbConnectionString)
	if err := server.Start(); err != nil {
		log.Fatalf("Error while starting server: %+v", err)
	}
}
