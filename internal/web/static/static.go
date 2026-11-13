// Package static embeds the assets for the interactive envoy extproc demo frontend.
package static

import "embed"

//go:embed *.html *.js *.css
var FS embed.FS
