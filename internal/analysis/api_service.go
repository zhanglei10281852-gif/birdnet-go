package analysis

import (
	"context"

	"github.com/tphakala/birdnet-go/internal/analysis/processor"
	"github.com/tphakala/birdnet-go/internal/api"
	apiv2 "github.com/tphakala/birdnet-go/internal/api/v2"
	"github.com/tphakala/birdnet-go/internal/app"
	"github.com/tphakala/birdnet-go/internal/audiocore"
	"github.com/tphakala/birdnet-go/internal/audiocore/engine"
	"github.com/tphakala/birdnet-go/internal/backup"
	"github.com/tphakala/birdnet-go/internal/conf"
	"github.com/tphakala/birdnet-go/internal/errors"
	"github.com/tphakala/birdnet-go/internal/health"
	"github.com/tphakala/birdnet-go/internal/imageprovider"
	"github.com/tphakala/birdnet-go/internal/logger"
	"github.com/tphakala/birdnet-go/internal/monitor"
	"github.com/tphakala/birdnet-go/internal/notification"
	"github.com/tphakala/birdnet-go/internal/observability"
	"github.com/tphakala/birdnet-go/internal/security"
	"github.com/tphakala/birdnet-go/internal/speciesindex"
	"github.com/tphakala/birdnet-go/internal/suncalc"
	"github.com/tphakala/birdnet-go/internal/telemetry"
)

// speciesIndexSetter is the optional interface a datastore implements to accept
// the orchestrator-owned species-name index. It cannot live on datastore.Interface
// because speciesindex imports datastore (that would close an import cycle), so the
// analysis package asserts it here. The v2-only datastore implements it; the legacy
// DataStore does not and keeps its own label-seeded maps.
type speciesIndexSetter interface {
	SetSpeciesIndex(svc *speciesindex.Service)
}

// installSpeciesIndex hands the datastore the orchestrator-owned species-name index
// when it implements speciesIndexSetter. A datastore that does not (the legacy
// DataStore) is left on its own label-seeded maps. Taking `any` keeps the optional
// assertion the single point of coupling and makes the wiring unit-testable.
func installSpeciesIndex(dataStore any, svc *speciesindex.Service) {
	if setter, ok := dataStore.(speciesIndexSetter); ok {
		setter.SetSpeciesIndex(svc)
	}
}

// apiServerServiceName is the service name used for logging and diagnostics.
const apiServerServiceName = "api-server"

// APIServerService manages the HTTP API server, processor, and related subsystems
// as an app.Service. It owns the lifecycle of the API server, processor, bird image
// cache, SunCalc, OAuth2 server, system monitor, and the control/audio-level channels.
type APIServerService struct {
	settings   *conf.Settings
	bnAnalyzer *BirdNETAnalyzer
	dbService  *DatabaseService
	metrics    *observability.Metrics
	engine     *engine.AudioEngine

	server         *api.Server
	proc           *processor.Processor
	birdImageCache *imageprovider.BirdImageCache
	sunCalc        *suncalc.SunCalc
	oauth2Server   *security.OAuth2Server
	systemMonitor  *monitor.SystemMonitor
	controlChan    chan string
	audioLevelChan chan audiocore.AudioLevelData

	// Backup subsystem lifecycle. The scheduler must be stopped (cancelling
	// and waiting for any in-flight runBackup) before the state manager is
	// closed, so a scheduled backup can never write to a closed state store
	// during shutdown.
	backupManager   *backup.Manager
	backupScheduler *backup.Scheduler
	backupState     *backup.StateManager

	// cancel stops background goroutines owned by subsystems (e.g. the OAuth2
	// server's periodic token cleanup). Created in Start, invoked in Stop.
	cancel context.CancelFunc
}

// NewAPIServerService creates a new APIServerService with the given dependencies.
// The service is not started; call Start() to initialize all subsystems.
func NewAPIServerService(settings *conf.Settings, bnAnalyzer *BirdNETAnalyzer, dbService *DatabaseService, metrics *observability.Metrics, audioEngine *engine.AudioEngine) *APIServerService {
	return &APIServerService{
		settings:   settings,
		bnAnalyzer: bnAnalyzer,
		dbService:  dbService,
		metrics:    metrics,
		engine:     audioEngine,
	}
}

// Name returns a human-readable identifier for logging and diagnostics.
func (s *APIServerService) Name() string {
	return apiServerServiceName
}

// Start initializes and starts the API server and all dependent subsystems.
// It fails fast if required dependencies (DataStore, BirdNET) are not available.
//
//nolint:gocognit // Orchestration function that initializes multiple subsystems in sequence.
func (s *APIServerService) Start(ctx context.Context) error {
	// If Start fails after creating resources, clean up to prevent leaks.
	// The App framework only calls Stop() on services that started successfully,
	// so the failing service must clean up after itself.
	startSucceeded := false
	defer func() {
		if !startSucceeded {
			// Best-effort cleanup of partially initialized resources.
			// Stop the backup scheduler before closing its state store so an
			// interrupted run is persisted as recoverable, not orphaned.
			s.stopBackupSystem()
			if s.cancel != nil {
				s.cancel()
				s.cancel = nil
			}
			if s.systemMonitor != nil {
				s.systemMonitor.Stop()
				s.systemMonitor = nil
			}
			if s.server != nil {
				_ = s.server.ShutdownWithContext(context.Background())
				s.server = nil
			}
			if s.proc != nil {
				_ = s.proc.ShutdownWithContext(context.Background())
				s.proc = nil
			}
		}
	}()

	// Fail fast: verify dependencies are initialized by upstream services.
	if s.dbService == nil || s.dbService.DataStore() == nil {
		return errors.Newf("api-server requires an initialized datastore; database service must be started first").
			Component("analysis.api_service").
			Category(errors.CategorySystem).
			Context("operation", "start_precondition_check").
			Build()
	}
	if s.bnAnalyzer == nil || s.bnAnalyzer.BirdNET() == nil {
		return errors.Newf("api-server requires an initialized birdnet model; birdnet-analyzer service must be started first").
			Component("analysis.api_service").
			Category(errors.CategorySystem).
			Context("operation", "start_precondition_check").
			Build()
	}

	dataStore := s.dbService.DataStore()
	bn := s.bnAnalyzer.BirdNET()

	// Install the orchestrator-owned species-name index on the datastore so its
	// common-name resolution reads the same snapshot the orchestrator rebuilds from
	// the union of loaded labels. This runs before processor.New (and thus before
	// the audio sources and the first detection save), earlier than the old startup
	// name-resolver wiring in NewControlMonitor. The legacy DataStore does not
	// implement the setter and stays on its own maps.
	installSpeciesIndex(dataStore, bn.SpeciesIndex())

	// Update BirdNET model loaded metric.
	UpdateBirdNETModelLoadedMetric(s.metrics.BirdNET, bn)

	// Initialize bird image cache.
	s.birdImageCache = initBirdImageCache(s.settings, dataStore, s.metrics)

	// Create SunCalc for sunrise/sunset calculations.
	s.sunCalc = newStationSunCalc(s.settings)

	// Create processor.
	s.proc = processor.New(s.settings, dataStore, bn, s.metrics, s.birdImageCache, GetLogger())
	s.proc.SetSunCalc(s.sunCalc)

	// Initialize backup system (optional; failure is non-fatal).
	backupLog := logger.Global().Module("backup")
	backupManager, backupScheduler, backupState, err := initializeBackupSystem(s.settings, backupLog)
	if err != nil {
		backupLog.Error("Failed to initialize backup system", logger.Error(err))
		GetLogger().Warn("backup system initialization failed",
			logger.Error(err),
			logger.String("operation", "backup_system_init"))
	} else {
		s.backupManager = backupManager
		s.backupScheduler = backupScheduler
		s.backupState = backupState
		s.proc.SetBackupManager(backupManager)
		s.proc.SetBackupScheduler(backupScheduler)
	}

	// Initialize async services (event bus, notification workers, telemetry workers).
	// This is fatal: the system cannot operate without these.
	if err := telemetry.InitializeAsyncSystems(); err != nil {
		GetLogger().Error("failed to initialize critical async services",
			logger.Error(err),
			logger.String("operation", "initialize_async_systems"))
		return errors.New(err).
			Component("analysis.api_service").
			Category(errors.CategorySystem).
			Context("operation", "initialize_async_systems").
			Build()
	}

	// Initialize system monitor (optional; failure is non-fatal).
	s.systemMonitor = initializeSystemMonitor(s.settings)

	// Create channels.
	s.controlChan = make(chan string, 1)
	s.audioLevelChan = make(chan audiocore.AudioLevelData, 100)

	// Derive a service lifecycle context, cancelled when the service stops (or
	// when the parent context is cancelled), so background goroutines owned by
	// subsystems (the OAuth2 server's periodic token cleanup) terminate on
	// shutdown instead of running for the process lifetime.
	serviceCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel

	// Create OAuth2 server. Session/auth cookies must be Secure only when clients
	// reach the app over HTTPS; derive that from the effective web-server TLS
	// config, which knows the AutoTLS redirect default and cert presence that the
	// persisted settings alone cannot express.
	webCfg := api.ConfigFromSettings(s.settings)
	s.oauth2Server = security.NewOAuth2Server(serviceCtx, webCfg.SessionCookiesSecure(s.settings))

	// Create and start the HTTP API server.
	GetLogger().Info("starting HTTP server")

	serverOpts := []api.ServerOption{
		api.WithDataStore(dataStore),
		api.WithBirdImageCache(s.birdImageCache),
		api.WithProcessor(s.proc),
		api.WithMetrics(s.metrics),
		api.WithControlChannel(s.controlChan),
		api.WithAudioLevelChannel(s.audioLevelChan),
		api.WithOAuth2Server(s.oauth2Server),
		api.WithSunCalc(s.sunCalc),
		api.WithV2Manager(s.dbService.V2Manager()),
		api.WithAudioEngine(s.engine),
	}
	if buf := health.GlobalErrorBuffer(); buf != nil {
		serverOpts = append(serverOpts, api.WithHealthErrorBuffer(buf))
	}
	if mm := s.bnAnalyzer.ModelManager(); mm != nil {
		serverOpts = append(serverOpts, api.WithModelManager(mm))
	}

	apiServer, err := api.New(
		s.settings,
		serverOpts...,
	)
	if err != nil {
		return errors.New(err).
			Component("analysis.api_service").
			Category(errors.CategoryHTTP).
			Context("operation", "create_http_server").
			Build()
	}
	s.server = apiServer
	s.server.Start()

	// Wire shutdown requester into API controller for restart endpoints.
	if appInstance := app.GetGlobal(); appInstance != nil {
		s.server.APIController().SetShutdownRequester(appInstance)
	}

	// Surface results-queue detection drops on the System Health page by
	// bridging them from the analysis pipeline into the diagnostics health
	// store owned by the API controller. The store is the bridge because
	// internal/api/v2 must not import internal/analysis (import cycle):
	// analysis pushes drops into the store, ResultsQueueDropCheck reads them.
	if ctrl := s.server.APIController(); ctrl != nil {
		SetResultsQueueDropHealthSink(ctrl.HealthMetricsStore(), ctrl.HealthEventBuffer())
	}

	startSucceeded = true
	return nil
}

// newStationSunCalc builds the sun calculator the API service shares with the processor, quiet
// hours, the nighttime scheduler and the API. It follows the live station location, so all of
// them pick up a location change without a restart.
func newStationSunCalc(settings *conf.Settings) *suncalc.SunCalc {
	return suncalc.NewSunCalcWithSource(conf.LiveLocation(settings))
}

// Stop gracefully shuts down the API server and owned subsystems.
// It is safe to call before Start() or multiple times.
func (s *APIServerService) Stop(ctx context.Context) error {
	log := GetLogger()

	// Cancel the service lifecycle context first so background goroutines owned
	// by subsystems (the OAuth2 server's periodic token cleanup) stop promptly.
	if s.cancel != nil {
		s.cancel()
		s.cancel = nil
	}

	// Stop the backup scheduler (cancelling and waiting for any in-flight
	// scheduled or manual backup) and only then close its state store. This
	// runs before the HTTP server drains and before the database service
	// stops, so no backup can keep writing after shutdown began and the
	// interrupted plan is durably recorded for restart recovery.
	s.stopBackupSystem()

	// Stop system monitor.
	if s.systemMonitor != nil {
		log.Info("stopping system monitor",
			logger.String("operation", "shutdown_system_monitor"))
		s.systemMonitor.Stop()
		s.systemMonitor = nil
	}

	// Shutdown HTTP server (drain in-flight requests before stopping processors).
	if s.server != nil {
		log.Info("shutting down HTTP server",
			logger.String("operation", "shutdown_http_server"))
		if err := s.server.ShutdownWithContext(ctx); err != nil {
			log.Warn("error shutting down HTTP server",
				logger.Error(err),
				logger.String("operation", "shutdown_http_server"))
		}
		s.server = nil
	}

	// Close control channel to signal goroutines selecting on it.
	if s.controlChan != nil {
		log.Info("closing control channel",
			logger.String("operation", "close_control_channel"))
		close(s.controlChan)
		s.controlChan = nil
	}

	// Shutdown processor (MQTT, job queue, thresholds).
	if s.proc != nil {
		log.Info("shutting down processor",
			logger.String("operation", "shutdown_processor"))
		if err := s.proc.ShutdownWithContext(ctx); err != nil {
			log.Warn("processor shutdown error",
				logger.Error(err),
				logger.String("operation", "shutdown_processor"))
		}
		s.proc = nil
	}

	// Detach the results-queue drop health sink so it does not retain the
	// controller's metrics store after shutdown. A subsequent Start re-wires it.
	SetResultsQueueDropHealthSink(nil, nil)

	// Stop notification service (after processor, which may send final notifications).
	if notification.IsInitialized() {
		log.Info("stopping notification service",
			logger.String("operation", "shutdown_notification_service"))
		if service := notification.GetService(); service != nil {
			service.Stop()
		}
	}

	// Close audio level channel for clean SSE client shutdown.
	if s.audioLevelChan != nil {
		close(s.audioLevelChan)
		s.audioLevelChan = nil
	}

	return nil
}

// Processor returns the detection processor, or nil if not yet started.
func (s *APIServerService) Processor() *processor.Processor {
	return s.proc
}

// Metrics returns the metrics instance, or nil if not configured.
func (s *APIServerService) Metrics() *observability.Metrics {
	return s.metrics
}

// APIController returns the v2 API controller, or nil if not yet started.
func (s *APIServerService) APIController() *apiv2.Controller {
	if s.server == nil {
		return nil
	}
	return s.server.APIController()
}

// ControlChan returns the control channel for restart/reload signaling,
// or nil if not yet started.
func (s *APIServerService) ControlChan() chan string {
	return s.controlChan
}

// AudioLevelChan returns the channel for audio level data updates,
// or nil if not yet started.
func (s *APIServerService) AudioLevelChan() chan audiocore.AudioLevelData {
	return s.audioLevelChan
}

// SunCalc returns the sunrise/sunset calculator, or nil if not yet started.
func (s *APIServerService) SunCalc() *suncalc.SunCalc {
	return s.sunCalc
}

// ReconfigureMonitoring stops the current SystemMonitor (if any) and recreates
// it from current settings. Called by ControlMonitor on hot-reload.
func (s *APIServerService) ReconfigureMonitoring() {
	if s.systemMonitor != nil {
		s.systemMonitor.Stop()
		s.systemMonitor = nil
	}
	s.systemMonitor = initializeSystemMonitor(conf.Setting())
}

// stopBackupSystem converges the backup scheduler, in-flight backup runs and
// the persistent state store. The scheduler is stopped first: it cancels the
// lifecycle context and waits for the run goroutine, whose final state write
// therefore lands before StateManager.Close. The method is safe to call when
// backup initialization failed or was skipped and is idempotent so Start
// rollback and Stop can both invoke it.
func (s *APIServerService) stopBackupSystem() {
	log := GetLogger()
	if s.backupScheduler != nil {
		log.Info("stopping backup scheduler",
			logger.String("operation", "shutdown_backup_scheduler"))
		s.backupScheduler.Stop()
		s.backupScheduler = nil
	}
	if s.backupState != nil {
		log.Info("closing backup state manager",
			logger.String("operation", "shutdown_backup_state"))
		if err := s.backupState.Close(); err != nil {
			log.Warn("backup state close error", logger.Error(err))
		}
		s.backupState = nil
	}
	// Manager holds no persistent resources; drop the reference once the
	// scheduler that could invoke it has stopped.
	s.backupManager = nil
}

// initializeBackupSystem sets up the backup manager, scheduler and persistent
// state manager.
func initializeBackupSystem(settings *conf.Settings, backupLog logger.Logger) (*backup.Manager, *backup.Scheduler, *backup.StateManager, error) {
	backupLog.Info("Initializing backup system...")

	stateManager, err := backup.NewStateManager(backupLog)
	if err != nil {
		return nil, nil, nil, errors.New(err).
			Component("analysis.api_service").
			Category(errors.CategorySystem).
			Context("operation", "initialize_backup_state_manager").
			Build()
	}

	// Use settings.Version for the app version
	backupManager, err := backup.NewManager(settings, backupLog, stateManager, settings.Version)
	if err != nil {
		return nil, nil, nil, errors.New(err).
			Component("analysis.api_service").
			Category(errors.CategorySystem).
			Context("operation", "initialize_backup_manager").
			Build()
	}
	backupScheduler, err := backup.NewScheduler(backupManager, backupLog, stateManager)
	if err != nil {
		return nil, nil, nil, errors.New(err).
			Component("analysis.api_service").
			Category(errors.CategorySystem).
			Context("operation", "initialize_backup_scheduler").
			Build()
	}

	// Load schedule for backupScheduler if backup is enabled
	switch {
	case settings.Backup.Enabled && len(settings.Backup.Schedules) > 0:
		backupLog.Info("Loading backup schedule from configuration")
		if err := backupScheduler.LoadFromConfig(&settings.Backup); err != nil {
			// Log the error but don't necessarily stop initialization
			backupLog.Error("Failed to load backup schedule from config", logger.Error(err))
		}
	case settings.Backup.Enabled:
		// This case is reached if backup is enabled but no schedules are defined.
		backupLog.Info("Backup enabled, but no schedules configured.")
	default:
		// This case is reached if backup is disabled.
		backupLog.Info("Backup system is disabled.")
	}

	// Start backupManager and backupScheduler if backup is enabled
	if settings.Backup.Enabled {
		backupLog.Info("Starting backup manager")
		if err := backupManager.Start(); err != nil {
			// Log the error but don't necessarily stop initialization
			backupLog.Error("Failed to start backup manager", logger.Error(err))
		}
		backupLog.Info("Starting backup scheduler")
		backupScheduler.Start() // Start the scheduler
	}

	backupLog.Info("Backup system initialized.")
	return backupManager, backupScheduler, stateManager, nil
}

// initializeSystemMonitor initializes and starts the system resource monitor if enabled
func initializeSystemMonitor(settings *conf.Settings) *monitor.SystemMonitor {
	GetLogger().Info("initializeSystemMonitor called",
		logger.Bool("monitoring_enabled", settings.Realtime.Monitoring.Enabled),
		logger.Int("check_interval", settings.Realtime.Monitoring.CheckInterval),
	)

	if !settings.Realtime.Monitoring.Enabled {
		GetLogger().Info("system monitoring is disabled")
		return nil
	}

	systemMonitor := monitor.NewSystemMonitor(settings)
	systemMonitor.Start()

	GetLogger().Info("system resource monitoring initialized",
		logger.Int("interval", settings.Realtime.Monitoring.CheckInterval))
	return systemMonitor
}
