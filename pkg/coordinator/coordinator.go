package coordinator

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
	"github.com/jackc/pgx/v4/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const shutdownTimeout = 5 * time.Second
const defaultMaxHeartbeatMisses = 1
const scanInterval = 10 * time.Second

type CoordinatorServer struct {
	pb.UnimplementedCoordinatorServiceServer
	port                string
	netListener         net.Listener
	grpcServer          *grpc.Server
	workerPool          map[uint32]*workerInfo
	workerPoolMutex     sync.Mutex
	workerPoolKeys      []uint32
	workerPoolKeysMutex sync.RWMutex
	roundRobinIndex     uint32
	maxHeartbeatMisses  uint8
	heartbeatInterval   time.Duration
	dbConnectionString  string
	pool                *pgxpool.Pool
	ctx                 context.Context
	cancel              context.CancelFunc
	wg                  sync.WaitGroup
}

type workerInfo struct {
	heartbeatMissCount  uint8
	address             string
	grpcConn            *grpc.ClientConn
	workerServiceClient pb.WorkerServiceClient
}

func NewServer(port, dbConnectionString string) *CoordinatorServer {
	ctx, cancel := context.WithCancel(context.Background())
	return &CoordinatorServer{
		port:               port,
		dbConnectionString: dbConnectionString,
		workerPool:         make(map[uint32]*workerInfo),
		maxHeartbeatMisses: defaultMaxHeartbeatMisses,
		heartbeatInterval:  common.HeartbeatInterval,
		ctx:                ctx,
		cancel:             cancel,
	}
}

func (s *CoordinatorServer) Start() error {
	go s.manageWorkerPool()

	if err := s.startGRPCServer(); err != nil {
		return fmt.Errorf("gRPC server start failed: %w", err)
	}

	var err error
	s.pool, err = common.ConnectToDatabase(s.ctx, s.dbConnectionString)
	if err != nil {
		return err
	}

	go s.scanDatabase()

	return s.awaitShutdown()
}

func (s *CoordinatorServer) startGRPCServer() error {
	var err error
	s.netListener, err = net.Listen("tcp", s.port)
	if err != nil {
		return err
	}

	log.Printf("Starting gRPC server on %s\n", s.port)
	s.grpcServer = grpc.NewServer()
	pb.RegisterCoordinatorServiceServer(s.grpcServer, s)

	go func() {
		if err := s.grpcServer.Serve(s.netListener); err != nil {
			log.Fatalf("gRPC server failed: %v", err)
		}
	}()

	return nil
}

func (s *CoordinatorServer) awaitShutdown() error {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	return s.Stop()
}

func (s *CoordinatorServer) Stop() error {
	s.cancel()
	s.wg.Wait()

	s.workerPoolMutex.Lock()
	defer s.workerPoolMutex.Unlock()
	for _, worker := range s.workerPool {
		if worker.grpcConn != nil {
			worker.grpcConn.Close()
		}
	}

	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}

	if s.netListener != nil {
		s.netListener.Close()
	}

	s.pool.Close()
	return nil
}

func (s *CoordinatorServer) SendHeartbeat(ctx context.Context, in *pb.SendHeartbeatRequest) (*pb.SendHeartbeatResponse, error) {
	s.workerPoolMutex.Lock()
	defer s.workerPoolMutex.Unlock()

	workerID := in.GetWorkerID()

	if worker, ok := s.workerPool[workerID]; ok {
		worker.heartbeatMissCount = 0
	} else {
		log.Println("Registering worker:", workerID)
		conn, err := grpc.NewClient(in.GetAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}

		s.workerPool[workerID] = &workerInfo{
			address:             in.GetAddress(),
			grpcConn:            conn,
			workerServiceClient: pb.NewWorkerServiceClient(conn),
		}

		s.workerPoolKeysMutex.Lock()
		defer s.workerPoolKeysMutex.Unlock()

		s.workerPoolKeys = make([]uint32, 0, len(s.workerPool))
		for k := range s.workerPool {
			s.workerPoolKeys = append(s.workerPoolKeys, k)
		}

		log.Println("Registered worker:", workerID)
	}

	return &pb.SendHeartbeatResponse{Acknowledged: true}, nil
}

func (s *CoordinatorServer) UpdateTaskStatus(ctx context.Context, req *pb.UpdateTaskStatusRequest) (*pb.UpdateTaskStatusResponse, error) {
	status := req.GetStatus()
	taskId := req.GetTaskID()
	var timestamp time.Time
	var column string

	switch status {
	case pb.TaskStatus_STARTED:
		timestamp = time.Unix(req.GetStartedAt(), 0)
		column = "started_at"
	case pb.TaskStatus_COMPLETE:
		timestamp = time.Unix(req.GetCompletedAt(), 0)
		column = "completed_at"
	case pb.TaskStatus_FAILED:
		timestamp = time.Unix(req.GetFailedAt(), 0)
		column = "failed_at"
	default:
		return nil, fmt.Errorf("invalid status: %v", status)
	}

	sqlStatement := fmt.Sprintf("UPDATE tasks SET %s = $1 WHERE id = $2", column)
	_, err := s.pool.Exec(ctx, sqlStatement, timestamp, taskId)
	if err != nil {
		log.Printf("Could not update task status for task %s: %v", taskId, err)
		return nil, err
	}

	return &pb.UpdateTaskStatusResponse{Success: true}, nil
}

func (s *CoordinatorServer) getNextWorker() *workerInfo {
	s.workerPoolKeysMutex.RLock()
	defer s.workerPoolKeysMutex.RUnlock()

	workerCount := len(s.workerPoolKeys)
	if workerCount == 0 {
		return nil
	}

	worker := s.workerPool[s.workerPoolKeys[s.roundRobinIndex%uint32(workerCount)]]
	s.roundRobinIndex++
	return worker
}

func (s *CoordinatorServer) submitTaskToWorker(task *pb.TaskRequest) error {
	worker := s.getNextWorker()
	if worker == nil {
		return fmt.Errorf("no workers available")
	}

	_, err := worker.workerServiceClient.SubmitTask(context.Background(), task)
	return err
}

func (s *CoordinatorServer) scanDatabase() {
	ticker := time.NewTicker(scanInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			go s.executeAllScheduledTasks()
		case <-s.ctx.Done():
			log.Println("Shutting down database scanner.")
			return
		}
	}
}

func (s *CoordinatorServer) executeAllScheduledTasks() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		log.Printf("Unable to start transaction: %v\n", err)
		return
	}

	defer func() {
		if err := tx.Rollback(ctx); err != nil && err.Error() != "tx is closed" {
			log.Printf("Failed to rollback transaction: %v\n", err)
		}
	}()

	rows, err := tx.Query(ctx, `SELECT id, command FROM tasks WHERE scheduled_at < (NOW() + INTERVAL '30 seconds') AND picked_at IS NULL ORDER BY scheduled_at FOR UPDATE SKIP LOCKED`)
	if err != nil {
		log.Printf("Error executing query: %v\n", err)
		return
	}
	defer rows.Close()

	var tasks []*pb.TaskRequest
	for rows.Next() {
		var id, command string
		if err := rows.Scan(&id, &command); err != nil {
			log.Printf("Failed to scan row: %v\n", err)
			continue
		}
		tasks = append(tasks, &pb.TaskRequest{TaskId: id, Data: command})
	}

	if err := rows.Err(); err != nil {
		log.Printf("Error iterating rows: %v\n", err)
		return
	}

	for _, task := range tasks {
		if err := s.submitTaskToWorker(task); err != nil {
			log.Printf("Failed to submit task %s: %v\n", task.GetTaskId(), err)
			continue
		}

		if _, err := tx.Exec(ctx, `UPDATE tasks SET picked_at = NOW() WHERE id = $1`, task.GetTaskId()); err != nil {
			log.Printf("Failed to update task %s: %v\n", task.GetTaskId(), err)
			continue
		}
	}

	if err := tx.Commit(ctx); err != nil {
		log.Printf("Failed to commit transaction: %v\n", err)
	}
}

func (s *CoordinatorServer) manageWorkerPool() {
	s.wg.Add(1)
	defer s.wg.Done()

	ticker := time.NewTicker(time.Duration(s.maxHeartbeatMisses) * s.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.removeInactiveWorkers()
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *CoordinatorServer) removeInactiveWorkers() {
	s.workerPoolMutex.Lock()
	defer s.workerPoolMutex.Unlock()

	for workerID, worker := range s.workerPool {
		if worker.heartbeatMissCount > s.maxHeartbeatMisses {
			log.Printf("Removing inactive worker: %d\n", workerID)
			worker.grpcConn.Close()
			delete(s.workerPool, workerID)

			s.workerPoolKeysMutex.Lock()
			s.workerPoolKeys = make([]uint32, 0, len(s.workerPool))
			for k := range s.workerPool {
				s.workerPoolKeys = append(s.workerPoolKeys, k)
			}
			s.workerPoolKeysMutex.Unlock()
		} else {
			worker.heartbeatMissCount++
		}
	}
}
