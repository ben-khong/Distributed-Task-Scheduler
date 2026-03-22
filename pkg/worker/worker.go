package worker

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/ben-khong/Distributed-Task-Scheduler/pkg/common"
	pb "github.com/ben-khong/Distributed-Task-Scheduler/pkg/grpcapi"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type WorkerServer struct {
	pb.UnimplementedWorkerServiceServer
	id                       uint32
	port                     string
	coordinatorAddress       string
	netListener              net.Listener
	grpcServer               *grpc.Server
	coordinatorConnection    *grpc.ClientConn
	coordinatorServiceClient pb.CoordinatorServiceClient // lowercase - unexported
	heartbeatInterval        time.Duration
	taskQueue                chan *pb.TaskRequest
	taskMap                  map[string]*pb.TaskRequest
	mu                       sync.Mutex
	ctx                      context.Context
	cancel                   context.CancelFunc
	wg                       sync.WaitGroup
}

func NewServer(port, coordinatorAddress string) *WorkerServer {
	id := uuid.New().ID()
	ctx, cancel := context.WithCancel(context.Background())
	return &WorkerServer{
		id:                 id,
		port:               port,
		coordinatorAddress: coordinatorAddress,
		ctx:                ctx,
		cancel:             cancel,
		heartbeatInterval:  common.HeartbeatInterval,
		taskQueue:          make(chan *pb.TaskRequest, 100),
		taskMap:            make(map[string]*pb.TaskRequest),
	}
}

func (w *WorkerServer) Start() error {
	w.startWorkerPool(5)

	if err := w.connectToCoordinator(); err != nil {
		return fmt.Errorf("failed to connect to coordinator: %w", err)
	}

	go w.periodicHeartbeat()

	if err := w.startGRPCServer(); err != nil {
		return fmt.Errorf("gRPC server start failed: %w", err)
	}

	return w.awaitShutdown()
}

func (w *WorkerServer) awaitShutdown() error {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	return w.Stop()
}

func (w *WorkerServer) Stop() error {
	w.cancel()
	w.wg.Wait()

	if w.grpcServer != nil {
		w.grpcServer.GracefulStop()
	}

	if w.netListener != nil {
		if err := w.netListener.Close(); err != nil {
			log.Printf("Error closing listener: %v", err)
		}
	}

	if w.coordinatorConnection != nil {
		if err := w.coordinatorConnection.Close(); err != nil {
			log.Printf("Error closing coordinator connection: %v", err)
		}
	}

	log.Println("Worker server stopped")
	return nil
}

func (w *WorkerServer) connectToCoordinator() error {
	conn, err := grpc.NewClient(w.coordinatorAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	w.coordinatorConnection = conn
	w.coordinatorServiceClient = pb.NewCoordinatorServiceClient(conn)
	return nil
}

func (w *WorkerServer) sendHeartbeat() error {
	workerAddress := os.Getenv("WORKER_ADDRESS")
	if workerAddress == "" {
		workerAddress = w.netListener.Addr().String()
	} else {
		workerAddress += w.port
	}

	_, err := w.coordinatorServiceClient.SendHeartbeat(context.Background(), &pb.SendHeartbeatRequest{
		WorkerID: w.id,
		Address:  workerAddress,
	})
	return err
}

func (w *WorkerServer) periodicHeartbeat() {
	w.wg.Add(1)
	defer w.wg.Done()

	ticker := time.NewTicker(w.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := w.sendHeartbeat(); err != nil {
				log.Printf("Failed to send heartbeat: %v", err)
				return
			}
		case <-w.ctx.Done():
			return
		}
	}
}

func (w *WorkerServer) startGRPCServer() error {
	var err error

	if w.port == "" {
		w.netListener, err = net.Listen("tcp", ":0")
		w.port = fmt.Sprintf(":%d", w.netListener.Addr().(*net.TCPAddr).Port)
	} else {
		w.netListener, err = net.Listen("tcp", w.port)
	}

	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", w.port, err)
	}

	log.Printf("Starting worker server on %s\n", w.port)
	w.grpcServer = grpc.NewServer()
	pb.RegisterWorkerServiceServer(w.grpcServer, w)

	go func() {
		if err := w.grpcServer.Serve(w.netListener); err != nil {
			log.Fatalf("gRPC server failed: %v", err)
		}
	}()

	return nil
}

func (w *WorkerServer) SubmitTask(ctx context.Context, req *pb.TaskRequest) (*pb.TaskResponse, error) {
	log.Printf("Received task: %+v", req)

	w.mu.Lock()
	w.taskMap[req.GetTaskId()] = req
	w.mu.Unlock()

	w.taskQueue <- req

	return &pb.TaskResponse{
		TaskID:  req.GetTaskId(),
		Msg:     "Task was submitted",
		Success: true,
	}, nil
}

func (w *WorkerServer) startWorkerPool(numWorkers int) {
	for i := 0; i < numWorkers; i++ {
		w.wg.Add(1)
		go w.workerLoop()
	}
}

func (w *WorkerServer) workerLoop() {
	defer w.wg.Done()

	for {
		select {
		case task := <-w.taskQueue:
			go w.updateTaskStatus(task, pb.TaskStatus_STARTED)
			w.processTask(task)
			go w.updateTaskStatus(task, pb.TaskStatus_COMPLETE)
		case <-w.ctx.Done():
			return
		}
	}
}

func (w *WorkerServer) processTask(task *pb.TaskRequest) {
	log.Printf("Processing task: %+v", task)
	time.Sleep(5 * time.Second)
	log.Printf("Completed task: %+v", task)
}

func (w *WorkerServer) updateTaskStatus(task *pb.TaskRequest, status pb.TaskStatus) {
	w.coordinatorServiceClient.UpdateTaskStatus(context.Background(), &pb.UpdateTaskStatusRequest{
		TaskID:      task.GetTaskId(),
		Status:      status,
		StartedAt:   time.Now().Unix(),
		CompletedAt: time.Now().Unix(),
	})
}
