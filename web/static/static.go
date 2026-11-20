// Package static embeds the assets for the interactive envoy extproc demo frontend
// served by the standalone web asset server (cmd/web).
package static

import "embed"

//go:embed *.html *.js *.css
var FS embed.FS
