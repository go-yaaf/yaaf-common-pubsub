package test

import (
	"github.com/go-yaaf/yaaf-common/logger"
	"os"
	"testing"
)

func init() {
	logger.EnableJsonFormat(false)
	logger.SetLevel("DEBUG")
	logger.Init()
}

func skipCI(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping testing in CI environment")
	}
}
