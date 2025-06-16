package supervisor

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/natefinch/lumberjack"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gopkg.in/yaml.v3"
)

const (
	debounceDelay         = 500 * time.Millisecond
	graceTimeout          = 5 * time.Second
	crashLoopThreshold    = 5
	crashLoopWindow       = 1 * time.Minute
	healthPort            = "9999"
	defaultLogDir         = "./log/myapp"
	defaultSupervisorLog  = defaultLogDir + "/supervisor.log"
	defaultChildLog       = defaultLogDir + "/child.log"
	defaultPIDFile        = "/tmp/supervisor.pid"
	defaultRestartBackoff = 1 * time.Second
	defaultMaxBackoff     = 30 * time.Second
)

var (
	restartCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "supervisor_restart_total",
			Help: "Total number of times the supervisor restarted the child.",
		},
		[]string{"reason"},
	)
	crashCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "supervisor_child_crash_total",
			Help: "Total number of times the child has crashed.",
		},
	)
	uptimeGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "supervisor_uptime_seconds",
			Help: "Supervisor uptime in seconds.",
		},
	)
)

func init() {
	prometheus.MustRegister(restartCounter, crashCounter, uptimeGauge)
}

type Config struct {
	EnvPaths []string `yaml:"envPaths" json:"envPaths"`
}

// loadConfig reads yaml config from the given path.
func loadConfig(configPath string) (*Config, error) {
	var cfg Config
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}
	if strings.Contains(configPath, ".yaml") || strings.Contains(configPath, ".yml") {
		data = bytes.TrimPrefix(data, []byte("\xef\xbb\xbf")) // Remove UTF-8 BOM if present
		if err := yaml.Unmarshal(data, &cfg); err == nil {
			return &cfg, nil
		}
	}

	if strings.Contains(configPath, ".json") {
		data = bytes.TrimPrefix(data, []byte("\xef\xbb\xbf")) // Remove UTF-8 BOM if present
		if err := json.Unmarshal(data, &cfg); err == nil {
			return &cfg, nil
		}
	}
	return nil, fmt.Errorf("failed to load config from %s: unsupported format", configPath)
}

type Supervisor struct {
	cmdLock        sync.Mutex
	currentCmd     *exec.Cmd
	restartEvent   chan string
	done           chan struct{}
	backoff        time.Duration
	watcher        *fsnotify.Watcher
	crashTimes     []time.Time
	crashTimesLock sync.Mutex
	startTime      time.Time
	httpServer     *http.Server
	callback       func(ctx context.Context) error
	childCommand   string
	envPath        string
	lastEnvHash    string
	lastOSEnv      map[string]string
	enableRestart  bool
}

func Run(configPath string, callback func(ctx context.Context) error) {
	cfg, err := loadConfig(configPath)
	if err != nil {
		log.Fatalf("Failed to load config from %s: %v", configPath, err)
	}
	if len(cfg.EnvPaths) == 0 {
		log.Fatal("no envPaths specified in config.yaml")
	}

	if os.Getenv("RUN_AS_CHILD") == "1" {
		setupLogging(defaultSupervisorLog)
		slog.Info("Child: starting callback")
		ctx, cancel := context.WithCancel(context.Background())
		sigC := make(chan os.Signal, 1)
		signal.Notify(sigC, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			<-sigC
			slog.Info("Child: shutdown signal received, cancelling context")
			cancel()
		}()
		if err := callback(ctx); err != nil {
			slog.Error("Child: callback returned error", slog.String("err", err.Error()))
			os.Exit(1)
		}
		return
	}
	setupLogging(defaultSupervisorLog)
	checkOrCreatePIDFile(defaultPIDFile)
	defer removePIDFile(defaultPIDFile)
	slog.Info("Supervisor: starting (library mode)")
	s := &Supervisor{
		restartEvent:   make(chan string, 1),
		done:           make(chan struct{}),
		backoff:        defaultRestartBackoff,
		startTime:      time.Now(),
		callback:       callback,
		envPath:        strings.Join(cfg.EnvPaths, ","),
		lastOSEnv:      make(map[string]string),
		childCommand:   "",
		crashTimes:     nil,
		currentCmd:     nil,
		httpServer:     nil,
		watcher:        nil,
		lastEnvHash:    "",
		crashTimesLock: sync.Mutex{},
		cmdLock:        sync.Mutex{},
	}
	// Initialize enableRestart from environment variable (default true)
	if val := os.Getenv("ENABLE_RESTART"); val == "false" {
		s.enableRestart = false
	} else {
		s.enableRestart = true
	}
	s.computeEnvHash()
	s.captureOSEnv()
	s.initWatcher()
	defer s.watcher.Close()
	go s.watchFiles()
	go s.watchEnvAndOSEnv()
	go s.spawnAndMonitor()
	go s.startMetricsServer()
	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
	for {
		select {
		case sig := <-sigC:
			switch sig {
			case syscall.SIGHUP:
				slog.Info("Supervisor: SIGHUP received, scheduling reload")
				s.queueRestart("sighup")
			default:
				slog.Info("Supervisor: shutdown signal received, tearing down child and exiting")
				close(s.done)
				s.killChild()
				return
			}
		case <-s.done:
			return
		}
	}
}

func Execute() {
	cmdFlag := flag.String("cmd", "", "Command to run as child (e.g. \"go run app.go\")")
	enableRestartFlag := flag.Bool("enable_restart", true, "Enable supervisor restarts if changes are detected")
	configFlag := flag.String("config", "config.yaml", "Path to YAML configuration file")
	flag.Parse()
	if *cmdFlag == "" {
		fmt.Fprintln(os.Stderr, "Error: --cmd is required")
		os.Exit(1)
	}
	cfg, err := loadConfig(*configFlag)
	if err == nil && len(cfg.EnvPaths) == 0 {
		log.Fatal("no envPaths specified in config.yaml")
	}
	setupLogging(defaultSupervisorLog)
	checkOrCreatePIDFile(defaultPIDFile)
	defer removePIDFile(defaultPIDFile)
	slog.Info("Supervisor: starting (standalone mode)")
	s := &Supervisor{
		restartEvent:   make(chan string, 1),
		done:           make(chan struct{}),
		backoff:        defaultRestartBackoff,
		startTime:      time.Now(),
		callback:       nil,
		envPath:        strings.Join(cfg.EnvPaths, ","),
		childCommand:   *cmdFlag,
		lastOSEnv:      make(map[string]string),
		crashTimes:     nil,
		currentCmd:     nil,
		httpServer:     nil,
		watcher:        nil,
		lastEnvHash:    "",
		crashTimesLock: sync.Mutex{},
		cmdLock:        sync.Mutex{},
	}
	s.enableRestart = *enableRestartFlag
	s.computeEnvHash()
	s.captureOSEnv()
	s.initStandaloneWatcher()
	defer s.watcher.Close()
	go s.watchFiles()
	go s.watchEnvAndOSEnv()
	go s.spawnAndMonitorStandalone()
	go s.startMetricsServer()
	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
	for {
		select {
		case sig := <-sigC:
			switch sig {
			case syscall.SIGHUP:
				slog.Info("Supervisor: SIGHUP received, scheduling reload")
				s.queueRestart("sighup")
			default:
				slog.Info("Supervisor: shutdown signal received, tearing down child and exiting")
				close(s.done)
				s.killChild()
				return
			}
		case <-s.done:
			return
		}
	}
}

func setupLogging(logPath string) {
	dir := filepath.Dir(logPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "Could not create log dir '%s': %v\n", dir, err)
		os.Exit(1)
	}
	fileLogger := &lumberjack.Logger{
		Filename:   logPath,
		MaxSize:    10,
		MaxBackups: 5,
		MaxAge:     28,
		Compress:   true,
	}
	mw := io.MultiWriter(os.Stdout, fileLogger)
	handler := slog.NewTextHandler(mw, &slog.HandlerOptions{AddSource: false})
	slog.SetDefault(slog.New(handler))
}

func checkOrCreatePIDFile(pidFile string) {
	if _, err := os.Stat(pidFile); err == nil {
		slog.Error("PID file already exists; another instance may be running", slog.String("file", pidFile))
		os.Exit(1)
	}
	pid := os.Getpid()
	if err := os.WriteFile(pidFile, []byte(fmt.Sprintf("%d\n", pid)), 0o644); err != nil {
		slog.Error("Failed to write PID file", slog.String("err", err.Error()))
		os.Exit(1)
	}
}

func removePIDFile(pidFile string) {
	_ = os.Remove(pidFile)
}

func (s *Supervisor) computeEnvHash() {
	files := strings.Split(s.envPath, ",")
	sort.Strings(files)
	var buf bytes.Buffer
	for _, file := range files {
		file = strings.TrimSpace(file)
		data, err := os.ReadFile(file)
		if err != nil {

			continue
		}
		buf.WriteString("file:" + file + "\n")
		buf.Write(data)
		buf.WriteString("\n")
	}
	sum := sha256.Sum256(buf.Bytes())
	s.lastEnvHash = hex.EncodeToString(sum[:])
}

func (s *Supervisor) captureOSEnv() {
	m := make(map[string]string)
	for _, kv := range os.Environ() {
		parts := strings.SplitN(kv, "=", 2)
		if len(parts) == 2 {
			m[parts[0]] = parts[1]
		}
	}
	s.lastOSEnv = m
}

func (s *Supervisor) detectOSEnvChanges() bool {
	changed := false
	curr := make(map[string]string)
	for _, kv := range os.Environ() {
		parts := strings.SplitN(kv, "=", 2)
		if len(parts) == 2 {
			curr[parts[0]] = parts[1]
		}
	}
	for k, v := range curr {
		if old, ok := s.lastOSEnv[k]; !ok {
			slog.Info("OS env added", slog.String("key", k), slog.String("val", v))
			changed = true
		} else if old != v {
			slog.Info("OS env changed", slog.String("key", k), slog.String("from", old), slog.String("to", v))
			changed = true
		}
	}
	for k := range s.lastOSEnv {
		if _, ok := curr[k]; !ok {
			slog.Info("OS env removed", slog.String("key", k))
			changed = true
		}
	}
	if changed {
		s.lastOSEnv = curr
	}
	return changed
}

func (s *Supervisor) initWatcher() {
	var err error
	s.watcher, err = fsnotify.NewWatcher()
	if err != nil {
		slog.Error("Failed to create fsnotify watcher", slog.String("err", err.Error()))
		os.Exit(1)
	}
	envPaths := strings.Split(s.envPath, ",")
	for _, p := range envPaths {
		p = strings.TrimSpace(p)
		abs, err := filepath.Abs(p)
		if err != nil {
			slog.Error("Unable to resolve env path", slog.String("path", p), slog.String("err", err.Error()))
			os.Exit(1)
		}
		fi, err := os.Stat(abs)
		if err != nil {
			slog.Error("Cannot stat env path", slog.String("path", abs), slog.String("err", err.Error()))
			os.Exit(1)
		}
		if fi.IsDir() {
			// Recursively watch the directory and its subdirectories.
			filepath.WalkDir(abs, func(path string, d os.DirEntry, err error) error {
				if err != nil {
					slog.Error("Error walking path", slog.String("path", path), slog.String("err", err.Error()))
					return err
				}
				if d.IsDir() {
					if err := s.watcher.Add(path); err != nil {
						slog.Error("Failed to watch directory", slog.String("dir", path), slog.String("err", err.Error()))
					} else {
						slog.Info("Watching directory", slog.String("dir", path))
					}
				}
				return nil
			})
		} else {
			// It's a file; watch its containing directory.
			dir := filepath.Dir(abs)
			if err := s.watcher.Add(dir); err != nil {
				slog.Error("Failed to watch file directory", slog.String("dir", dir), slog.String("err", err.Error()))
				os.Exit(1)
			}
			slog.Info("Watching file directory", slog.String("dir", dir))
		}
	}
	if bin, err := os.Executable(); err == nil {
		if abs, err := filepath.Abs(bin); err == nil {
			dir := filepath.Dir(abs)
			if err := s.watcher.Add(dir); err != nil {
				slog.Error("Failed to watch supervisor binary directory", slog.String("dir", dir), slog.String("err", err.Error()))
				os.Exit(1)
			}
			slog.Info("Watching supervisor binary directory", slog.String("dir", dir))
		}
	}
	if home := os.Getenv("HOME"); home != "" {
		awsConfig := filepath.Join(home, ".aws", "config")
		awsCreds := filepath.Join(home, ".aws", "credentials")
		for _, f := range []string{awsConfig, awsCreds} {
			if abs, err := filepath.Abs(f); err == nil {
				dir := filepath.Dir(abs)
				if err := s.watcher.Add(dir); err == nil {
					slog.Info("Watching AWS config/credentials directory", slog.String("dir", dir))
				}
			}
		}
	}
}

func (s *Supervisor) initStandaloneWatcher() {
	s.initWatcher()
}

func (s *Supervisor) watchFiles() {
	timer := time.NewTimer(0)
	<-timer.C
	var mu sync.Mutex
	debounce := func(reason string) {
		select {
		case <-s.done:
			return
		default:
		}
		mu.Lock()
		defer mu.Unlock()
		timer.Reset(debounceDelay)
		go func() {
			<-timer.C
			s.queueRestart(reason)
		}()
	}
	for {
		select {
		case event := <-s.watcher.Events:
			if event.Op&(fsnotify.Write|fsnotify.Create|fsnotify.Rename) != 0 {
				slog.Info("File event detected", slog.String("file", event.Name), slog.String("op", event.Op.String()))
				debounce(event.Name)
			}
		case err := <-s.watcher.Errors:
			slog.Error("Watcher error", slog.String("err", err.Error()))
		case <-s.done:
			return
		}
	}
}

func (s *Supervisor) watchEnvAndOSEnv() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if s.detectOSEnvChanges() {
				s.queueRestart("os_env")
			}
			oldHash := s.lastEnvHash
			s.computeEnvHash()
			if s.lastEnvHash != oldHash {
				slog.Info("Environment file change detected (hash changed)")
				// s.queueRestart("env_file")
			}
		case <-s.done:
			return
		}
	}
}

func (s *Supervisor) queueRestart(reason string) {
	if !s.enableRestart {
		slog.Info("Restart disabled; ignoring restart event", slog.String("reason", reason))
		return
	}
	select {
	case <-s.done:
		return
	case s.restartEvent <- reason:
	default:
	}
}

func (s *Supervisor) spawnAndMonitor() {
	for {
		select {
		case <-s.done:
			return
		default:
		}
		s.resetBackoff()
		if err := s.startChild(); err != nil {
			slog.Error("Failed to start child process", slog.String("err", err.Error()))
			os.Exit(1)
		}
		exitCh := make(chan error, 1)
		go func() {
			exitCh <- s.currentCmd.Wait()
		}()
		select {
		case reason := <-s.restartEvent:
			slog.Info("Supervisor: restarting child", slog.String("reason", reason))
			restartCounter.WithLabelValues(reason).Inc()
			s.killChild()
			continue
		case err := <-exitCh:
			if err != nil {
				slog.Error("Child exited with error", slog.String("err", err.Error()))
				crashCounter.Inc()
				if !s.recordCrashAndCheckLoop() {
					slog.Error("Too many child crashes in short window; supervisor exiting")
					os.Exit(1)
				}
			} else {
				slog.Info("Child exited cleanly")
			}
			if s.backoffAndSleep() {
				continue
			}
			return

		case <-s.done:
			s.killChild()
			return
		}
	}
}

func (s *Supervisor) startChild() error {
	s.cmdLock.Lock()
	defer s.cmdLock.Unlock()
	binPath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("unable to get executable path: %w", err)
	}
	args := os.Args[1:]
	cmd := exec.Command(binPath, args...)
	env := os.Environ()
	env = append(env, "RUN_AS_CHILD=1")
	cmd.Env = env
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}
	if err := os.MkdirAll(filepath.Dir(defaultChildLog), 0o755); err != nil {
		slog.Error("Failed to create child log directory", slog.String("err", err.Error()))
		return err
	}
	childLogger := &lumberjack.Logger{
		Filename:   defaultChildLog,
		MaxSize:    10,
		MaxBackups: 5,
		MaxAge:     28,
		Compress:   true,
	}
	cmd.Stdout = io.MultiWriter(os.Stdout, childLogger)
	cmd.Stderr = io.MultiWriter(os.Stderr, childLogger)
	if err := cmd.Start(); err != nil {
		return err
	}
	s.currentCmd = cmd
	slog.Info("Supervisor: spawned child process", slog.Int("pid", cmd.Process.Pid))
	return nil
}

func (s *Supervisor) spawnAndMonitorStandalone() {
	for {
		select {
		case <-s.done:
			return
		default:
		}
		s.resetBackoff()
		if err := s.startChildStandalone(); err != nil {
			slog.Error("Failed to start child process", slog.String("err", err.Error()))
			os.Exit(1)
		}
		exitCh := make(chan error, 1)
		go func() {
			exitCh <- s.currentCmd.Wait()
		}()
		select {
		case reason := <-s.restartEvent:
			slog.Info("Supervisor: restarting child", slog.String("reason", reason))
			restartCounter.WithLabelValues(reason).Inc()
			s.killChild()
			continue
		case err := <-exitCh:
			if err != nil {
				slog.Error("Child exited with error", slog.String("err", err.Error()))
				crashCounter.Inc()
				if !s.recordCrashAndCheckLoop() {
					slog.Error("Too many child crashes in short window; supervisor exiting")
					os.Exit(1)
				}
			} else {
				slog.Info("Child exited cleanly")
			}
			if s.backoffAndSleep() {
				continue
			}
			return

		case <-s.done:
			s.killChild()
			return
		}
	}
}

func (s *Supervisor) startChildStandalone() error {
	s.cmdLock.Lock()
	defer s.cmdLock.Unlock()
	if s.childCommand == "" {
		return errors.New("no child command specified")
	}
	cmd := exec.Command("/bin/sh", "-c", s.childCommand)
	cmd.Env = os.Environ()
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}
	if err := os.MkdirAll(filepath.Dir(defaultChildLog), 0o755); err != nil {
		slog.Error("Failed to create child log directory", slog.String("err", err.Error()))
		return err
	}
	childLogger := &lumberjack.Logger{
		Filename:   defaultChildLog,
		MaxSize:    10,
		MaxBackups: 5,
		MaxAge:     28,
		Compress:   true,
	}
	cmd.Stdout = io.MultiWriter(os.Stdout, childLogger)
	cmd.Stderr = io.MultiWriter(os.Stderr, childLogger)
	if err := cmd.Start(); err != nil {
		return err
	}
	s.currentCmd = cmd
	slog.Info("Supervisor: spawned standalone child", slog.Int("pid", cmd.Process.Pid), slog.String("cmd", s.childCommand))
	return nil
}

func (s *Supervisor) killChild() {
	s.cmdLock.Lock()
	defer s.cmdLock.Unlock()
	if s.currentCmd == nil || s.currentCmd.Process == nil {
		return
	}
	pid := s.currentCmd.Process.Pid
	_ = syscall.Kill(-pid, syscall.SIGTERM)
	done := make(chan struct{})
	go func() {
		s.currentCmd.Wait()
		close(done)
	}()
	select {
	case <-done:
		slog.Info("Supervisor: child terminated gracefully")
	case <-time.After(graceTimeout):
		slog.Warn("Supervisor: child did not exit in time; sending SIGKILL")
		_ = syscall.Kill(-pid, syscall.SIGKILL)
		<-done
	}
	s.currentCmd = nil
}

func (s *Supervisor) resetBackoff() {
	s.backoff = defaultRestartBackoff
}

func (s *Supervisor) backoffAndSleep() bool {
	d := s.backoff
	if s.backoff < defaultMaxBackoff {
		s.backoff *= 2
	}
	slog.Info("Supervisor: backing off before restart", slog.Duration("duration", d))
	select {
	case <-time.After(d):
		return true
	case <-s.done:
		return false
	}
}

func (s *Supervisor) recordCrashAndCheckLoop() bool {
	s.crashTimesLock.Lock()
	defer s.crashTimesLock.Unlock()
	now := time.Now()
	windowStart := now.Add(-crashLoopWindow)
	newList := s.crashTimes[:0]
	for _, t := range s.crashTimes {
		if t.After(windowStart) {
			newList = append(newList, t)
		}
	}
	s.crashTimes = newList
	s.crashTimes = append(s.crashTimes, now)
	return len(s.crashTimes) <= crashLoopThreshold
}

// ManualRestart to manually control the child process.
func (s *Supervisor) ManualRestart() {
	// Initiate a manual restart.
	s.queueRestart("manual")
}

func (s *Supervisor) ManualShutdown() {
	// Immediately shutdown the child process.
	s.killChild()
}

func (s *Supervisor) ManualStart() error {
	s.cmdLock.Lock()
	defer s.cmdLock.Unlock()
	if s.currentCmd != nil {
		slog.Info("Child application is already running")
		return nil
	}
	return s.startChild()
}

// Modify startMetricsServer to add endpoints to manually control the child.
func (s *Supervisor) startMetricsServer() {
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				uptimeGauge.Set(time.Since(s.startTime).Seconds())
			case <-s.done:
				return
			}
		}
	}()
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	// New endpoints for child management:
	mux.HandleFunc("/child/restart", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}
		s.ManualRestart()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("Child restart initiated"))
	})
	mux.HandleFunc("/child/shutdown", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}
		s.ManualShutdown()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("Child shutdown initiated"))
	})
	mux.HandleFunc("/child/start", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}
		if err := s.ManualStart(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("Child started"))
	})
	slog.Info("Supervisor: metrics/health and control endpoints listening", slog.String("port", healthPort))
	server := &http.Server{
		Addr:    ":" + healthPort,
		Handler: mux,
	}
	s.httpServer = server
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("Supervisor: metrics server ListenAndServe error", slog.String("err", err.Error()))
		}
	}()
	<-s.done
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		slog.Warn("Supervisor: metrics server shutdown error", slog.String("err", err.Error()))
	} else {
		slog.Info("Supervisor: metrics server shut down cleanly")
	}
}
