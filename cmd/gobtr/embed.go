//go:build embed

package main

import (
	"embed"
	"io/fs"

	"github.com/elee1766/gobtr/pkg/api"
)

//go:embed all:dist
var embeddedDist embed.FS

func init() {
	subFS, err := fs.Sub(embeddedDist, "dist")
	if err != nil {
		panic(err)
	}
	api.EmbeddedFS = subFS
}
