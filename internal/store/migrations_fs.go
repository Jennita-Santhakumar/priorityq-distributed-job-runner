package store

import "embed"

// MigrationsFS embeds the SQL migration files so cmd/migrate can apply them
// without relying on a filesystem path being present at runtime (works
// identically whether run from source or from the compiled Docker binary).
//
//go:embed migrations/*.sql
var MigrationsFS embed.FS
