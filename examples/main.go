package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/joho/godotenv"
	"gopkg.in/yaml.v3"

	"github.com/oarkflow/supervisor"
)

var envFiles = ".env"

func main() {
	os.Setenv("ENABLE_RESTART", "true")
	supervisor.Run("config.json", Run)
}

func Run(ctx context.Context) error {
	if err := setupEnvFiles(envFiles); err != nil {
		slog.Error("Error loading env files", slog.String("err", err.Error()))
	}
	app := fiber.New()
	app.Get("/", func(c *fiber.Ctx) error {
		return c.SendString("Fiber app is running!")
	})
	app.Get("/readyz", func(c *fiber.Ctx) error {
		return c.SendStatus(http.StatusOK)
	})
	errCh := make(chan error, 1)
	go func() {
		port := os.Getenv("PORT")
		if port == "" {
			port = "3000"
		}
		slog.Info("Child starting Fiber server", slog.String("port", port))
		errCh <- app.Listen(":" + port)
	}()

	<-ctx.Done()
	slog.Info("Draining Fiber server connections and shutting down")
	_ = app.Shutdown()
	return <-errCh
}

func setupEnvFiles(paths string) error {
	parts := strings.Split(paths, ",")
	for _, raw := range parts {
		path := strings.TrimSpace(raw)
		if path == "" {
			continue
		}
		if _, err := os.Stat(path); err != nil {
			slog.Warn("Env file not found or inaccessible", slog.String("file", path), slog.String("err", err.Error()))
			continue
		}
		switch ext := strings.ToLower(filepath.Ext(path)); ext {
		case ".env":
			if err := godotenv.Load(path); err != nil {
				slog.Warn("Failed to load .env file", slog.String("file", path), slog.String("err", err.Error()))
			}
		case ".json":
			if err := loadJSONEnv(path); err != nil {
				slog.Warn("Failed to load JSON env file", slog.String("file", path), slog.String("err", err.Error()))
			}
		case ".yaml", ".yml":
			if err := loadYAMLEnv(path); err != nil {
				slog.Warn("Failed to load YAML env file", slog.String("file", path), slog.String("err", err.Error()))
			}
		default:
			slog.Warn("Unsupported env file extension; skipped", slog.String("file", path))
		}
	}
	return nil
}

func loadJSONEnv(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var envMap any
	if err := json.Unmarshal(data, &envMap); err != nil {
		return err
	}
	fmt.Println(envMap)
	return nil
}

func loadYAMLEnv(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var envMap any
	if err := yaml.Unmarshal(data, &envMap); err != nil {
		return err
	}
	fmt.Println(envMap)
	return nil
}
