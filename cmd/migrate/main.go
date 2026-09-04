// Command migrate applies (or rolls back) database migrations. It is run
// as a one-shot Docker Compose service that gates the api/worker services'
// startup via service_completed_successfully, avoiding races against an
// unmigrated schema.
package main

import (
	"errors"
	"log"
	"os"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"

	"github.com/KabileshRajaselvan/task-queue-system/internal/config"
	"github.com/KabileshRajaselvan/task-queue-system/internal/store"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	direction := "up"
	if len(os.Args) > 1 {
		direction = os.Args[1]
	}

	src, err := iofs.New(store.MigrationsFS, "migrations")
	if err != nil {
		log.Fatalf("failed to load embedded migrations: %v", err)
	}
	m, err := migrate.NewWithSourceInstance("iofs", src, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("failed to init migrator: %v", err)
	}

	switch direction {
	case "up":
		err = m.Up()
	case "down":
		err = m.Down()
	default:
		log.Fatalf("unknown migrate direction %q (expected up or down)", direction)
	}

	if err != nil && !errors.Is(err, migrate.ErrNoChange) {
		log.Fatalf("migration failed: %v", err)
	}
	log.Println("migrations applied successfully")
}
