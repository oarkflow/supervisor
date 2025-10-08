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
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
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
	defaultCrashThreshold = 5
	defaultCrashWindow    = 1 * time.Minute
	defaultHealthPort     = "9999"
	defaultLogDir         = "./log/myapp"
	defaultSupervisorLog  = defaultLogDir + "/supervisor.log"
	defaultChildLog       = defaultLogDir + "/child.log"
	defaultPIDFile        = "/tmp/supervisor.pid"
	defaultRestartBackoff = 1 * time.Second
	defaultMaxBackoff     = 30 * time.Second
	jitterFraction        = 0.2 // +/- 20%
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
	childRunningGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "supervisor_child_running",
			Help: "1 if a child process is running, 0 otherwise.",
		},
	)
	backoffGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "supervisor_restart_backoff_seconds",
			Help: "Current restart backoff duration in seconds.",
		},
	)
)

func init() {
	prometheus.MustRegister(restartCounter, crashCounter, uptimeGauge, childRunningGauge, backoffGauge)
	rand.Seed(time.Now().UnixNano())
}

type Config struct {
	EnvPaths               []string      `yaml:"envPaths" json:"envPaths"`
	WatchPaths             []string      `yaml:"watchPaths" json:"watchPaths"`
	IncludeGlobs           []string      `yaml:"includeGlobs" json:"includeGlobs"`
	IgnoreGlobs            []string      `yaml:"ignoreGlobs" json:"ignoreGlobs"`
	RestartOnEnvHashChange bool          `yaml:"restartOnEnvHashChange" json:"restartOnEnvHashChange"`
	EnableRestart          *bool         `yaml:"enableRestart" json:"enableRestart"`
	HealthPort             string        `yaml:"healthPort" json:"healthPort"`
	LogDir                 string        `yaml:"logDir" json:"logDir"`
	PIDFile                string        `yaml:"pidFile" json:"pidFile"`
	RestartBackoff         time.Duration `yaml:"restartBackoff" json:"restartBackoff"`
	MaxBackoff             time.Duration `yaml:"maxBackoff" json:"maxBackoff"`
	CrashLoopThreshold     int           `yaml:"crashLoopThreshold" json:"crashLoopThreshold"`
	CrashLoopWindow        time.Duration `yaml:"crashLoopWindow" json:"crashLoopWindow"`
	ControlToken           string        `yaml:"controlToken" json:"controlToken"`
	TLSCert                string        `yaml:"tlsCert" json:"tlsCert"`
	TLSKey                 string        `yaml:"tlsKey" json:"tlsKey"`
	HealthCheckURL         string        `yaml:"healthCheckURL" json:"healthCheckURL"`
	HealthCheckInterval    time.Duration `yaml:"healthCheckInterval" json:"healthCheckInterval"`
	LogLevel               string        `yaml:"logLevel" json:"logLevel"`
}

// loadConfig: supports YAML or JSON; returns empty config (not nil) if not found.
func loadConfig(configPath string) (*Config, error) {
	if configPath == "" {
		return &Config{}, nil
	}
	data, err := os.ReadFile(configPath)
	if err != nil {
		// Not fatal; return empty config to keep standalone flexible
		return &Config{}, nil
	}
	cfg := &Config{}
	data = bytes.TrimPrefix(data, []byte("\xef\xbb\xbf"))
	var umErr error
	if strings.HasSuffix(configPath, ".yaml") || strings.HasSuffix(configPath, ".yml") {
		umErr = yaml.Unmarshal(data, cfg)
	} else if strings.HasSuffix(configPath, ".json") {
		umErr = json.Unmarshal(data, cfg)
	} else {
		// best effort: try yaml then json
		if err := yaml.Unmarshal(data, cfg); err != nil {
			umErr = json.Unmarshal(data, cfg)
		}
	}
	if umErr != nil {
		return nil, fmt.Errorf("failed to parse config: %w", umErr)
	}
	if err := validateConfig(cfg); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	return cfg, nil
}

func validateConfig(cfg *Config) error {
	if cfg.RestartBackoff < 0 {
		return errors.New("restartBackoff must be non-negative")
	}
	if cfg.MaxBackoff < 0 {
		return errors.New("maxBackoff must be non-negative")
	}
	if cfg.CrashLoopThreshold < 0 {
		return errors.New("crashLoopThreshold must be non-negative")
	}
	if cfg.CrashLoopWindow < 0 {
		return errors.New("crashLoopWindow must be non-negative")
	}
	if cfg.HealthCheckInterval < 0 {
		return errors.New("healthCheckInterval must be non-negative")
	}
	if cfg.HealthCheckURL != "" {
		if _, err := url.Parse(cfg.HealthCheckURL); err != nil {
			return fmt.Errorf("invalid healthCheckURL: %w", err)
		}
	}
	if cfg.TLSCert != "" && cfg.TLSKey == "" {
		return errors.New("tlsKey required if tlsCert is provided")
	}
	if cfg.TLSKey != "" && cfg.TLSCert == "" {
		return errors.New("tlsCert required if tlsKey is provided")
	}
	if cfg.LogLevel != "" {
		switch cfg.LogLevel {
		case "DEBUG", "INFO", "WARN", "ERROR":
		default:
			return errors.New("logLevel must be one of DEBUG, INFO, WARN, ERROR")
		}
	}
	return nil
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
	configPath     string

	// config-derived
	envPaths     []string
	watchPaths   []string
	includeGlobs []string
	ignoreGlobs  []string

	lastEnvHash   string
	lastOSEnv     map[string]string
	enableRestart bool

	healthPort    string
	logDir        string
	supervisorLog string
	childLog      string
	pidFile       string

	minBackoff  time.Duration
	maxBackoff  time.Duration
	crashThresh int
	crashWindow time.Duration

	controlToken           string
	restartOnEnvHashChange bool

	tlsCert             string
	tlsKey              string
	healthCheckURL      string
	healthCheckInterval time.Duration
	logLevel            string

	// status
	lastRestartReason string
	lastRestartAt     time.Time
}

// ---------- Public entrypoints ----------

func Run(configPath string, callback func(ctx context.Context) error) {
	cfg, err := loadConfig(configPath)
	if err != nil {
		log.Fatalf("Failed to load config from %s: %v", configPath, err)
	}
	s := newSupervisor(cfg, callback, "", configPath)
	s.bootstrap()
	s.mainLoop()
}

func Execute() {
	cmdFlag := flag.String("cmd", "", "Command to run as child (e.g. \"go run app.go\")")
	enableRestartFlag := flag.Bool("enable_restart", true, "Enable supervisor restarts if changes are detected")
	configFlag := flag.String("config", "config.yaml", "Path to YAML/JSON configuration file")
	flag.Parse()

	if *cmdFlag == "" {
		fmt.Fprintln(os.Stderr, "Error: --cmd is required")
		os.Exit(1)
	}
	cfg, err := loadConfig(*configFlag)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}
	s := newSupervisor(cfg, nil, *cmdFlag, *configFlag)
	// CLI flag overrides config
	if enableRestartFlag != nil {
		s.enableRestart = *enableRestartFlag
	}
	s.bootstrap()
	s.mainLoop()
}

// ---------- Construction / defaults ----------

func newSupervisor(cfg *Config, cb func(ctx context.Context) error, childCmd string, configPath string) *Supervisor {
	// defaults
	enableRestart := true
	if cfg.EnableRestart != nil {
		enableRestart = *cfg.EnableRestart
	} else if v := os.Getenv("ENABLE_RESTART"); v == "false" {
		enableRestart = false
	}
	healthPort := cfg.HealthPort
	if healthPort == "" {
		healthPort = defaultHealthPort
	}
	logDir := cfg.LogDir
	if logDir == "" {
		logDir = defaultLogDir
	}
	pidFile := cfg.PIDFile
	if pidFile == "" {
		pidFile = defaultPIDFile
	}
	minBackoff := cfg.RestartBackoff
	if minBackoff == 0 {
		minBackoff = defaultRestartBackoff
	}
	maxBackoff := cfg.MaxBackoff
	if maxBackoff == 0 {
		maxBackoff = defaultMaxBackoff
	}
	crashThresh := cfg.CrashLoopThreshold
	if crashThresh == 0 {
		crashThresh = defaultCrashThreshold
	}
	crashWindow := cfg.CrashLoopWindow
	if crashWindow == 0 {
		crashWindow = defaultCrashWindow
	}

	// sane ignore defaults
	ignore := cfg.IgnoreGlobs
	if len(ignore) == 0 {
		ignore = []string{
			"**/.git/**",
			"**/.idea/**",
			"**/.vscode/**",
			"**/*.swp",
			"**/*.tmp",
			"**/*.log",
			"**/*.db",
			"**/node_modules/**",
			"**/dist/**",
			"**/build/**",
		}
	}

	return &Supervisor{
		restartEvent:           make(chan string, 1),
		done:                   make(chan struct{}),
		backoff:                minBackoff,
		startTime:              time.Now(),
		callback:               cb,
		childCommand:           childCmd,
		configPath:             configPath,
		envPaths:               dedupStrings(cfg.EnvPaths),
		watchPaths:             dedupStrings(append(cfg.WatchPaths, cfg.EnvPaths...)),
		includeGlobs:           dedupStrings(cfg.IncludeGlobs),
		ignoreGlobs:            dedupStrings(ignore),
		lastOSEnv:              make(map[string]string),
		healthPort:             healthPort,
		logDir:                 logDir,
		supervisorLog:          filepath.Join(logDir, "supervisor.log"),
		childLog:               filepath.Join(logDir, "child.log"),
		pidFile:                pidFile,
		minBackoff:             minBackoff,
		maxBackoff:             maxBackoff,
		crashThresh:            crashThresh,
		crashWindow:            crashWindow,
		enableRestart:          enableRestart,
		controlToken:           firstNonEmpty(cfg.ControlToken, os.Getenv("SUPERVISOR_TOKEN")),
		restartOnEnvHashChange: cfg.RestartOnEnvHashChange,
		tlsCert:                cfg.TLSCert,
		tlsKey:                 cfg.TLSKey,
		healthCheckURL:         cfg.HealthCheckURL,
		healthCheckInterval:    cfg.HealthCheckInterval,
		logLevel:               cfg.LogLevel,
	}
}

func (s *Supervisor) bootstrap() {
	if os.Getenv("RUN_AS_CHILD") == "1" {
		// Child mode: log to child log
		setupLogging(s.childLog, "INFO") // default for child
		slog.Info("Child: starting callback")
		ctx, cancel := context.WithCancel(context.Background())
		sigC := make(chan os.Signal, 1)
		signal.Notify(sigC, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			<-sigC
			slog.Info("Child: shutdown signal received, cancelling context")
			cancel()
		}()
		if s.callback == nil {
			slog.Error("Child: callback is nil in library mode")
			os.Exit(1)
		}
		if err := s.callback(ctx); err != nil {
			slog.Error("Child: callback returned error", slog.String("err", err.Error()))
			os.Exit(1)
		}
		return
	}

	setupLogging(s.supervisorLog, s.logLevel)
	s.handlePIDFile() // robust handling for stale PID files

	slog.Info("Supervisor: starting",
		slog.String("mode", ifThenElse(s.callback != nil, "library", "standalone")),
		slog.String("healthPort", s.healthPort),
	)

	s.computeEnvHash()
	s.captureOSEnv()

	s.initWatcher()
	go s.watchLoop()

	go s.envPoller()
	if s.healthCheckURL != "" {
		go s.healthChecker()
	}
	go s.metricsServer()

	// choose spawn loop
	if s.callback != nil {
		go s.spawnAndMonitor()
	} else {
		go s.spawnAndMonitorStandalone()
	}
}

// ---------- Logging / PID ----------

func setupLogging(logPath string, logLevel string) {
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
	level := &slog.LevelVar{}
	switch logLevel {
	case "DEBUG":
		level.Set(slog.LevelDebug)
	case "INFO":
		level.Set(slog.LevelInfo)
	case "WARN":
		level.Set(slog.LevelWarn)
	case "ERROR":
		level.Set(slog.LevelError)
	default:
		level.Set(slog.LevelInfo)
	}
	handler := slog.NewTextHandler(mw, &slog.HandlerOptions{AddSource: false, Level: level})
	slog.SetDefault(slog.New(handler))
}

func (s *Supervisor) handlePIDFile() {
	// If existing, check liveness; otherwise create
	if b, err := os.ReadFile(s.pidFile); err == nil {
		var oldPID int
		_, _ = fmt.Sscanf(string(b), "%d", &oldPID)
		if oldPID > 0 && processAlive(oldPID) {
			slog.Error("PID file exists and process appears alive. Another instance may be running.",
				slog.String("file", s.pidFile), slog.Int("pid", oldPID))
			os.Exit(1)
		}
		slog.Warn("Stale PID file found; overwriting", slog.String("file", s.pidFile), slog.Int("pid", oldPID))
	}
	pid := os.Getpid()
	if err := os.WriteFile(s.pidFile, []byte(fmt.Sprintf("%d\n", pid)), 0o644); err != nil {
		slog.Error("Failed to write PID file", slog.String("err", err.Error()))
		os.Exit(1)
	}
	// ensure removal on exit
	go func() {
		<-s.done
		_ = os.Remove(s.pidFile)
	}()
}

func processAlive(pid int) bool {
	// On Unix, signal 0 checks liveness
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	return err == nil
}

// ---------- Env / hashing ----------

func (s *Supervisor) computeEnvHash() {
	files := dedupStrings(s.envPaths)
	sort.Strings(files)
	var buf bytes.Buffer
	for _, file := range files {
		file = strings.TrimSpace(file)
		data, err := os.ReadFile(file)
		if err != nil {
			// don't spam logs; just note once
			slog.Debug("Env file not readable", slog.String("file", file), slog.String("err", err.Error()))
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
	for k := range curr {
		if _, ok := s.lastOSEnv[k]; !ok {
			slog.Info("OS env added", slog.String("key", k))
			changed = true
		} else if s.lastOSEnv[k] != curr[k] {
			slog.Info("OS env changed", slog.String("key", k))
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

// ---------- Watchers ----------

func (s *Supervisor) initWatcher() {
	var err error
	s.watcher, err = fsnotify.NewWatcher()
	if err != nil {
		slog.Error("Failed to create fsnotify watcher", slog.String("err", err.Error()))
		os.Exit(1)
	}
	paths := s.watchPaths
	if len(paths) == 0 {
		// fall back to supervisor binary dir
		if bin, err := os.Executable(); err == nil {
			if abs, err := filepath.Abs(bin); err == nil {
				paths = []string{filepath.Dir(abs)}
			}
		}
	}
	for _, p := range paths {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		abs, err := filepath.Abs(p)
		if err != nil {
			slog.Error("Unable to resolve watch path", slog.String("path", p), slog.String("err", err.Error()))
			continue
		}
		fi, err := os.Stat(abs)
		if err != nil {
			slog.Error("Cannot stat watch path", slog.String("path", abs), slog.String("err", err.Error()))
			continue
		}
		if fi.IsDir() {
			filepath.WalkDir(abs, func(path string, d os.DirEntry, err error) error {
				if err != nil {
					return nil
				}
				if d.IsDir() {
					_ = s.watcher.Add(path)
				}
				return nil
			})
		} else {
			_ = s.watcher.Add(filepath.Dir(abs))
		}
	}

	// watch supervisor binary dir as a fallback
	if bin, err := os.Executable(); err == nil {
		if abs, err := filepath.Abs(bin); err == nil {
			_ = s.watcher.Add(filepath.Dir(abs))
		}
	}

	// watch AWS config dirs softly
	if home := os.Getenv("HOME"); home != "" {
		for _, f := range []string{filepath.Join(home, ".aws", "config"), filepath.Join(home, ".aws", "credentials")} {
			if abs, err := filepath.Abs(f); err == nil {
				_ = s.watcher.Add(filepath.Dir(abs))
			}
		}
	}
}

func (s *Supervisor) matchGlobs(name string, globs []string) bool {
	if len(globs) == 0 {
		return true // no filters
	}
	// minimal globber — use filepath.Match per segment
	name = filepath.ToSlash(name)
	for _, g := range globs {
		g = filepath.ToSlash(g)
		ok, _ := filepath.Match(g, name)
		if ok {
			return true
		}
	}
	return false
}

func (s *Supervisor) ignored(name string) bool {
	// ignore if matches any ignore glob
	name = filepath.ToSlash(name)
	for _, g := range s.ignoreGlobs {
		ok, _ := filepath.Match(g, name)
		if ok {
			return true
		}
	}
	return false
}

func (s *Supervisor) watchLoop() {
	// safe debounce: queue latest reason, fire once after calm period
	type dEvent struct{ reason string }
	debounceC := make(chan dEvent, 1)

	go func() {
		var last dEvent
		var timer *time.Timer
		for {
			select {
			case ev := <-debounceC:
				last = ev
				if timer == nil {
					timer = time.NewTimer(debounceDelay)
				} else {
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					timer.Reset(debounceDelay)
				}
			case <-func() <-chan time.Time {
				if timer != nil {
					return timer.C
				}
				// dormant channel
				ch := make(chan time.Time)
				return ch
			}():
				s.queueRestart(last.reason)
			case <-s.done:
				return
			}
		}
	}()

	for {
		select {
		case event := <-s.watcher.Events:
			op := event.Op
			name := event.Name

			if s.ignored(name) {
				continue
			}
			// include filter (if provided)
			if len(s.includeGlobs) > 0 && !s.matchGlobs(name, s.includeGlobs) {
				continue
			}

			// If a new directory is created, add it to watcher
			if op&fsnotify.Create != 0 {
				if fi, err := os.Stat(name); err == nil && fi.IsDir() {
					filepath.WalkDir(name, func(path string, d os.DirEntry, err error) error {
						if err == nil && d.IsDir() && !s.ignored(path) {
							_ = s.watcher.Add(path)
						}
						return nil
					})
				}
			}

			// If a directory is removed, remove it from watcher
			if op&fsnotify.Remove != 0 {
				if _, err := os.Stat(name); err != nil {
					// File/dir no longer exists, try to remove from watcher
					_ = s.watcher.Remove(name)
				}
			}

			if op&(fsnotify.Write|fsnotify.Create|fsnotify.Rename|fsnotify.Remove) != 0 {
				slog.Info("File event", slog.String("file", name), slog.String("op", op.String()))
				select {
				case debounceC <- dEvent{reason: name}:
				default:
					// drop if busy; latest will still be processed
				}
			}
		case err := <-s.watcher.Errors:
			if err != nil {
				slog.Error("Watcher error", slog.String("err", err.Error()))
			}
		case <-s.done:
			_ = s.watcher.Close()
			return
		}
	}
}

// ---------- Env polling ----------

func (s *Supervisor) envPoller() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if s.detectOSEnvChanges() {
				s.queueRestart("os_env")
			}
			old := s.lastEnvHash
			s.computeEnvHash()
			if s.lastEnvHash != old {
				slog.Info("Environment file hash changed")
				if s.restartOnEnvHashChange {
					s.queueRestart("env_file")
				}
			}
		case <-s.done:
			return
		}
	}
}

// ---------- Health checker ----------

func (s *Supervisor) healthChecker() {
	interval := s.healthCheckInterval
	if interval == 0 {
		interval = 30 * time.Second // default
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	client := &http.Client{Timeout: 10 * time.Second}
	for {
		select {
		case <-ticker.C:
			resp, err := client.Get(s.healthCheckURL)
			if err != nil || resp.StatusCode < 200 || resp.StatusCode >= 300 {
				slog.Warn("Health check failed", slog.String("url", s.healthCheckURL), slog.String("err", err.Error()))
				s.queueRestart("health_check")
			} else {
				resp.Body.Close()
			}
		case <-s.done:
			return
		}
	}
}

// ---------- Restart queue ----------

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
		// Already queued; keep last reason for status
	}
	s.lastRestartReason = reason
	s.lastRestartAt = time.Now()
}

// ---------- Spawn/monitor (library) ----------

func (s *Supervisor) spawnAndMonitor() {
	for {
		select {
		case <-s.done:
			return
		default:
		}
		s.resetBackoff()

		if err := s.startChildLibrary(); err != nil {
			slog.Error("Failed to start child (library mode)", slog.String("err", err.Error()))
			os.Exit(1)
		}
		exitCh := make(chan error, 1)
		go func() { exitCh <- s.currentCmd.Wait() }()

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
					close(s.done)
					return
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

func (s *Supervisor) startChildLibrary() error {
	s.cmdLock.Lock()
	defer s.cmdLock.Unlock()

	binPath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("unable to get executable path: %w", err)
	}
	args := os.Args[1:]
	cmd := exec.Command(binPath, args...)
	cmd.Env = append(os.Environ(), "RUN_AS_CHILD=1")
	if runtime.GOOS != "windows" {
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	}
	if err := os.MkdirAll(filepath.Dir(s.childLog), 0o755); err != nil {
		slog.Error("Failed to create child log directory", slog.String("err", err.Error()))
		return err
	}
	childLogger := &lumberjack.Logger{
		Filename:   s.childLog,
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
	childRunningGauge.Set(1)
	slog.Info("Supervisor: spawned child process", slog.Int("pid", cmd.Process.Pid))
	return nil
}

// ---------- Spawn/monitor (standalone) ----------

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
		go func() { exitCh <- s.currentCmd.Wait() }()

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
					close(s.done)
					return
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
	shell, flag := s.getShellAndFlag()
	cmd := exec.Command(shell, flag, s.childCommand)
	cmd.Env = os.Environ()
	if runtime.GOOS != "windows" {
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	}
	if err := os.MkdirAll(filepath.Dir(s.childLog), 0o755); err != nil {
		slog.Error("Failed to create child log directory", slog.String("err", err.Error()))
		return err
	}
	childLogger := &lumberjack.Logger{
		Filename:   s.childLog,
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
	childRunningGauge.Set(1)
	slog.Info("Supervisor: spawned standalone child", slog.Int("pid", cmd.Process.Pid), slog.String("cmd", s.childCommand))
	return nil
}

// ---------- Control ----------

func (s *Supervisor) killChild() {
	s.cmdLock.Lock()
	defer s.cmdLock.Unlock()
	if s.currentCmd == nil || s.currentCmd.Process == nil {
		return
	}
	pid := s.currentCmd.Process.Pid

	// Graceful first
	if runtime.GOOS != "windows" {
		_ = syscall.Kill(-pid, syscall.SIGTERM) // process group
	} else {
		_ = s.currentCmd.Process.Signal(os.Interrupt)
	}

	done := make(chan struct{})
	go func() {
		_, _ = s.currentCmd.Process.Wait()
		close(done)
	}()

	select {
	case <-done:
		slog.Info("Supervisor: child terminated gracefully")
	case <-time.After(graceTimeout):
		slog.Warn("Supervisor: child did not exit in time; sending SIGKILL")
		if runtime.GOOS != "windows" {
			_ = syscall.Kill(-pid, syscall.SIGKILL)
		} else {
			_ = s.currentCmd.Process.Kill()
		}
		<-done
	}
	s.currentCmd = nil
	childRunningGauge.Set(0)
}

func (s *Supervisor) resetBackoff() {
	s.backoff = s.minBackoff
	backoffGauge.Set(s.backoff.Seconds())
}

func (s *Supervisor) backoffAndSleep() bool {
	d := s.withJitter(s.backoff)
	if s.backoff < s.maxBackoff {
		s.backoff *= 2
		if s.backoff > s.maxBackoff {
			s.backoff = s.maxBackoff
		}
	}
	backoffGauge.Set(d.Seconds())
	slog.Info("Supervisor: backing off before restart", slog.Duration("duration", d))
	select {
	case <-time.After(d):
		return true
	case <-s.done:
		return false
	}
}

func (s *Supervisor) withJitter(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	// +/- jitterFraction
	f := 1 + (rand.Float64()*2-1)*jitterFraction
	return time.Duration(float64(d) * f)
}

func (s *Supervisor) recordCrashAndCheckLoop() bool {
	s.crashTimesLock.Lock()
	defer s.crashTimesLock.Unlock()
	now := time.Now()
	windowStart := now.Add(-s.crashWindow)
	newList := s.crashTimes[:0]
	for _, t := range s.crashTimes {
		if t.After(windowStart) {
			newList = append(newList, t)
		}
	}
	s.crashTimes = newList
	s.crashTimes = append(s.crashTimes, now)
	return len(s.crashTimes) <= s.crashThresh
}

// Manual controls
func (s *Supervisor) ManualRestart()  { s.queueRestart("manual") }
func (s *Supervisor) ManualShutdown() { s.killChild() }

func (s *Supervisor) ManualStart() error {
	s.cmdLock.Lock()
	defer s.cmdLock.Unlock()
	if s.currentCmd != nil {
		slog.Info("Child application is already running")
		return nil
	}
	// choose appropriate starter
	if s.callback != nil {
		return s.startChildLibrary()
	}
	return s.startChildStandalone()
}

// ---------- Metrics / HTTP ----------

func (s *Supervisor) metricsServer() {
	// uptime ticker
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
	// auth wrapper
	auth := func(h http.HandlerFunc) http.HandlerFunc {
		if s.controlToken == "" {
			return h
		}
		return func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("X-Supervisor-Token") != s.controlToken {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}
			h(w, r)
		}
	}

	mux.HandleFunc("/child/status", auth(func(w http.ResponseWriter, r *http.Request) {
		type status struct {
			Running           bool          `json:"running"`
			PID               int           `json:"pid"`
			UptimeSeconds     float64       `json:"uptimeSeconds"`
			CrashCountWindow  int           `json:"crashCountWindow"`
			CrashWindow       time.Duration `json:"crashWindow"`
			EnableRestart     bool          `json:"enableRestart"`
			LastRestartReason string        `json:"lastRestartReason"`
			LastRestartAt     time.Time     `json:"lastRestartAt"`
			BackoffSeconds    float64       `json:"backoffSeconds"`
		}
		pid := 0
		running := false
		if s.currentCmd != nil && s.currentCmd.Process != nil {
			pid = s.currentCmd.Process.Pid
			running = true
		}
		st := status{
			Running:           running,
			PID:               pid,
			UptimeSeconds:     time.Since(s.startTime).Seconds(),
			CrashCountWindow:  len(s.crashTimes),
			CrashWindow:       s.crashWindow,
			EnableRestart:     s.enableRestart,
			LastRestartReason: s.lastRestartReason,
			LastRestartAt:     s.lastRestartAt,
			BackoffSeconds:    s.backoff.Seconds(),
		}
		_ = json.NewEncoder(w).Encode(st)
	}))

	mux.HandleFunc("/child/restart", auth(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}
		s.ManualRestart()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("Child restart initiated"))
	}))

	mux.HandleFunc("/child/shutdown", auth(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}
		s.ManualShutdown()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("Child shutdown initiated"))
	}))

	mux.HandleFunc("/child/start", auth(func(w http.ResponseWriter, r *http.Request) {
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
	}))

	mux.HandleFunc("/child/toggle-restart", auth(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}
		s.enableRestart = !s.enableRestart
		fmt.Fprintf(w, "enableRestart=%v", s.enableRestart)
	}))

	mux.HandleFunc("/reload", auth(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}
		cfg, err := loadConfig(s.configPath)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to reload config: %v", err), http.StatusInternalServerError)
			return
		}
		// Update reloadable fields
		s.envPaths = dedupStrings(cfg.EnvPaths)
		s.watchPaths = dedupStrings(append(cfg.WatchPaths, cfg.EnvPaths...))
		s.includeGlobs = dedupStrings(cfg.IncludeGlobs)
		s.ignoreGlobs = dedupStrings(cfg.IgnoreGlobs)
		s.restartOnEnvHashChange = cfg.RestartOnEnvHashChange
		s.controlToken = firstNonEmpty(cfg.ControlToken, os.Getenv("SUPERVISOR_TOKEN"))
		s.healthCheckURL = cfg.HealthCheckURL
		s.healthCheckInterval = cfg.HealthCheckInterval
		slog.Info("Config reloaded")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("Config reloaded"))
	}))
	server := &http.Server{Addr: ":" + s.healthPort, Handler: mux}
	s.httpServer = server

	go func() {
		var err error
		if s.tlsCert != "" && s.tlsKey != "" {
			err = server.ListenAndServeTLS(s.tlsCert, s.tlsKey)
		} else {
			err = server.ListenAndServe()
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("Supervisor: metrics server error", slog.String("err", err.Error()))
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

// ---------- Main loop / signals ----------

func (s *Supervisor) mainLoop() {
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

// ---------- Helpers ----------

func dedupStrings(in []string) []string {
	seen := map[string]struct{}{}
	var out []string
	for _, s := range in {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		if _, ok := seen[s]; !ok {
			seen[s] = struct{}{}
			out = append(out, s)
		}
	}
	return out
}

func ifThenElse[T any](cond bool, a, b T) T {
	if cond {
		return a
	}
	return b
}

func firstNonEmpty(v ...string) string {
	for _, s := range v {
		if strings.TrimSpace(s) != "" {
			return s
		}
	}
	return ""
}

func (s *Supervisor) getShellAndFlag() (string, string) {
	if runtime.GOOS == "windows" {
		return "cmd", "/C"
	}
	return "/bin/sh", "-c"
}
