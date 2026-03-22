package scheduler

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ben-khong/Distributed-Task-Scheduler/pkg/common"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4/pgxpool"
)

type CommandRequest struct {
	Command     string `json:"command"`
	ScheduledAt string `json:"scheduled_at"`
}

type Task struct {
	Id          string
	Command     string
	ScheduledAt pgtype.Timestamp
	PickedAt    pgtype.Timestamp
	StartedAt   pgtype.Timestamp
	CompletedAt pgtype.Timestamp
	FailedAt    pgtype.Timestamp
}

type SchedulerServer struct {
	port               string
	dbConnectionString string
	pool               *pgxpool.Pool
	httpServer         *http.Server
	ctx                context.Context
	cancel             context.CancelFunc
}

func NewServer(port string, dbConnectionString string) *SchedulerServer {
	ctx, cancel := context.WithCancel(context.Background())
	return &SchedulerServer{
		port:               port,
		dbConnectionString: dbConnectionString,
		ctx:                ctx,
		cancel:             cancel,
	}
}

func (s *SchedulerServer) Start() error {
	var err error
	s.pool, err = common.ConnectToDatabase(s.ctx, s.dbConnectionString)
	if err != nil {
		return err
	}
	http.HandleFunc("/schedule", s.handleScheduleTask)
	http.HandleFunc("/status", s.handleGetTaskStatus)
	s.httpServer = &http.Server{Addr: s.port}
	log.Printf("Starting scheduler server on %s\n", s.port)
	go func() {
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %s\n", err)
		}
	}()
	return s.awaitShutdown()
}

func (s *SchedulerServer) awaitShutdown() error {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	return s.Stop()
}

func (s *SchedulerServer) Stop() error {
	s.pool.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if s.httpServer != nil {
		return s.httpServer.Shutdown(ctx)
	}
	return nil
}

func (s *SchedulerServer) handleScheduleTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}
	var commandReq CommandRequest
	err := json.NewDecoder(r.Body).Decode(&commandReq)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	scheduledTime, err := time.Parse(time.RFC3339, commandReq.ScheduledAt)
	if err != nil {
		http.Error(w, "Invalid date format. Use ISO 8601 format.", http.StatusBadRequest)
		return
	}
	id, err := s.insertTaskIntoDB(context.Background(), Task{Command: commandReq.Command, ScheduledAt: pgtype.Timestamp{Time: scheduledTime}})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	response := struct {
		Command     string `json:"command"`
		ScheduledAt int64  `json:"scheduled_at"`
		TaskID      string `json:"task_id"`
	}{
		Command:     commandReq.Command,
		ScheduledAt: scheduledTime.Unix(),
		TaskID:      id,
	}
	jsonBytes, err := json.Marshal(response)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonBytes)
}

func (s *SchedulerServer) insertTaskIntoDB(ctx context.Context, task Task) (string, error) {
	var id string
	err := s.pool.QueryRow(ctx, "INSERT INTO tasks (command, scheduled_at) VALUES ($1, $2) RETURNING id", task.Command, task.ScheduledAt.Time).Scan(&id)
	return id, err
}

func (s *SchedulerServer) handleGetTaskStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Only GET method is allowed", http.StatusMethodNotAllowed)
		return
	}
	taskID := r.URL.Query().Get("task_id")
	if taskID == "" {
		http.Error(w, "Task ID is empty", http.StatusBadRequest)
		return
	}
	var task Task
	err := s.pool.QueryRow(context.Background(), "SELECT * FROM tasks WHERE id = $1", taskID).Scan(&task.Id, &task.Command, &task.ScheduledAt, &task.PickedAt, &task.StartedAt, &task.CompletedAt, &task.FailedAt)
	if err != nil {
		http.Error(w, "Failed to get task status", http.StatusInternalServerError)
		return
	}
	response := struct {
		TaskID      string `json:"task_id"`
		Command     string `json:"command"`
		ScheduledAt string `json:"scheduled_at,omitempty"`
		PickedAt    string `json:"picked_at,omitempty"`
		StartedAt   string `json:"started_at,omitempty"`
		CompletedAt string `json:"completed_at,omitempty"`
		FailedAt    string `json:"failed_at,omitempty"`
	}{
		TaskID:  task.Id,
		Command: task.Command,
	}

	if task.ScheduledAt.Status == 2 {
		response.ScheduledAt = task.ScheduledAt.Time.String()
	}
	if task.PickedAt.Status == 2 {
		response.PickedAt = task.PickedAt.Time.String()
	}
	if task.StartedAt.Status == 2 {
		response.StartedAt = task.StartedAt.Time.String()
	}
	if task.CompletedAt.Status == 2 {
		response.CompletedAt = task.CompletedAt.Time.String()
	}
	if task.FailedAt.Status == 2 {
		response.FailedAt = task.FailedAt.Time.String()
	}

	jsonBytes, err := json.Marshal(response)
	if err != nil {
		http.Error(w, "Failed to marshal JSON response", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonBytes)

}
