package main

import (
	"flag"
	"log"

	"github.com/ben-khong/Distributed-Task-Scheduler/pkg/worker"
)

var (
	workerPort      = flag.String("worker_port", "", "Port on which the Worker serves requests.")
	coordinatorAddr = flag.String("coordinator", ":8080", "Network address of the Coordinator.")
)

func main() {
	flag.Parse()
	w := worker.NewServer(*workerPort, *coordinatorAddr)
	if err := w.Start(); err != nil {
		log.Fatalf("Error while starting worker: %+v", err)
	}
}
