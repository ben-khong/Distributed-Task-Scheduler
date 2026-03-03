/* This file exists because both the scheduler and the coordinator need
to connect to the same Postgres database. Rather than duplicating the
connection logic and config-reading code in both packages, you put it
in one shared place. */

package common

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
)

const HeartbeatInterval = 5 * time.Second

func GetDBConnectionString() string {
	user := os.Getenv("POSTGRES_USER")
	password := os.Getenv("POSTGRES_PASSWORD")
	db := os.Getenv("POSTGRES_DB")
	host := os.Getenv("POSTGRES_HOST")

	var missing []string

	if user == "" {
		missing = append(missing, "POSTGRES_USER")
	}

	if password == "" {
		missing = append(missing, "POSTGRES_PASSWORD")
	}

	if db == "" {
		missing = append(missing, "POSTGRES_DB")
	}

	if len(missing) > 0 {
		log.Fatalf("The following required environmental variables are not set: %s", strings.Join(missing, ", "))
	}

	if host == "" {
		host = "localhost"
	}

	return fmt.Sprintf("postgres://%s:%s@%s:5432/%s", user, password, host, db)
}

func ConnectToDatabase(ctx context.Context, connectionString string) (*pgxpool.Pool, error) {
	var pool *pgxpool.Pool
	var err error

	for i := 0; i < 5; i++ {
		pool, err = pgxpool.Connect(ctx, connectionString)
		if err == nil {
			log.Printf("Connected to the database.")
			return pool, nil
		}
		log.Printf("Failed to connect, retrying in 5 seconds...")
		time.Sleep(5 * time.Second)

	}

	return nil, err
}
