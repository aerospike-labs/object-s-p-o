// Package version reports which commit a binary was built from, using the
// VCS information Go embeds at build time.
package version

import (
	"fmt"
	"runtime"
	"runtime/debug"
)

// String describes the build, e.g. "bec744a (2026-10-07T18:02:11Z) linux/amd64".
// A "+modified" suffix means the build had uncommitted changes.
func String() string {
	revision, built, modified := "unknown", "", false
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, s := range info.Settings {
			switch s.Key {
			case "vcs.revision":
				revision = s.Value
				if len(revision) > 7 {
					revision = revision[:7]
				}
			case "vcs.time":
				built = s.Value
			case "vcs.modified":
				modified = s.Value == "true"
			}
		}
	}
	if modified {
		revision += "+modified"
	}
	if built != "" {
		revision += " (" + built + ")"
	}
	return fmt.Sprintf("%s %s/%s", revision, runtime.GOOS, runtime.GOARCH)
}
