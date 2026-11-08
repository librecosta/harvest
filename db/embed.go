package db

import (
	"embed"
	"io/fs"
)

//go:embed migrations
var files embed.FS

var Migrations, _ = fs.Sub(files, "migrations")
