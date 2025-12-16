package config

import (
	"os"
	"path/filepath"
)

const (
	// AppName is the application name used in paths
	AppName = "gobtr"
)

// Config holds all application configuration.
type Config struct {
	// Paths
	DataDir   string // Base data directory (XDG_DATA_HOME/gobtr)
	ConfigDir string // Config directory (XDG_CONFIG_HOME/gobtr)
	CacheDir  string // Cache directory (XDG_CACHE_HOME/gobtr)

	// Derived paths
	DBPath       string // SQLite database path
	BTDUStoreDir string // btdu usage samples directory

	// Server
	APIAddress string

	// Logging
	LogLevel string
}

// New creates a new Config with values from environment or defaults.
func New() *Config {
	cfg := &Config{}

	// Base directories (XDG Base Directory Specification)
	cfg.DataDir = getDataDir()
	cfg.ConfigDir = getConfigDir()
	cfg.CacheDir = getCacheDir()

	// Ensure directories exist
	os.MkdirAll(cfg.DataDir, 0755)
	os.MkdirAll(cfg.ConfigDir, 0755)
	os.MkdirAll(cfg.CacheDir, 0755)

	// Derived paths
	cfg.DBPath = envOrDefault("GOBTR_DB_PATH", filepath.Join(cfg.DataDir, "gobtr.db"))
	cfg.BTDUStoreDir = envOrDefault("GOBTR_BTDU_DIR", filepath.Join(cfg.DataDir, "btdu"))

	// Server config
	cfg.APIAddress = envOrDefault("GOBTR_API_ADDRESS", ":8147")

	// Logging
	cfg.LogLevel = envOrDefault("GOBTR_LOG_LEVEL", "info")

	return cfg
}

// getDataDir returns the data directory following XDG spec.
// $XDG_DATA_HOME/gobtr or ~/.local/share/gobtr
func getDataDir() string {
	if dir := os.Getenv("XDG_DATA_HOME"); dir != "" {
		return filepath.Join(dir, AppName)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return filepath.Join(".", AppName, "data")
	}
	return filepath.Join(home, ".local", "share", AppName)
}

// getConfigDir returns the config directory following XDG spec.
// $XDG_CONFIG_HOME/gobtr or ~/.config/gobtr
func getConfigDir() string {
	if dir := os.Getenv("XDG_CONFIG_HOME"); dir != "" {
		return filepath.Join(dir, AppName)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return filepath.Join(".", AppName, "config")
	}
	return filepath.Join(home, ".config", AppName)
}

// getCacheDir returns the cache directory following XDG spec.
// $XDG_CACHE_HOME/gobtr or ~/.cache/gobtr
func getCacheDir() string {
	if dir := os.Getenv("XDG_CACHE_HOME"); dir != "" {
		return filepath.Join(dir, AppName)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return filepath.Join(".", AppName, "cache")
	}
	return filepath.Join(home, ".cache", AppName)
}

// envOrDefault returns the environment variable value or the default.
func envOrDefault(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

// SubPath returns a path under the data directory.
func (c *Config) SubPath(parts ...string) string {
	return filepath.Join(append([]string{c.DataDir}, parts...)...)
}
