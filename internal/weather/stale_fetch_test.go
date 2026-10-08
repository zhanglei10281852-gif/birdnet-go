package weather

// Regression coverage for the provider/location commit gate in fetchAndSave.
//
// The settings snapshot is copy-on-write and can be republished while a
// provider HTTP request is still in flight (site admin saves a new provider or
// location in the settings UI). A fetch carries the provider/location snapshot
// it started with; when the response returns, the cycle only commits if the
// live settings still match that snapshot. These tests pin the six scenarios
// required by the fix:
//
//  1. default fetch with no config change commits as before,
//  2. a late PROVIDER response is discarded (no persistence/telemetry) and the
//     new provider's next success commits the complete new snapshot,
//  3. the same for a late LOCATION response (including sunrise/sunset),
//  4. request timeout/network failure keeps the last good observation, reports
//     an explainable error/backoff, and a later cycle recovers; a late ERROR
//     result after a config switch is discarded without poisoning backoff,
//  5. duplicate on-demand refreshes serialize and never commit the old result,
//  6. a process restart (fresh Service over the same DB) commits only the
//     configuration it started with and enforces the gate on its own first
//     in-flight request.

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"

	"github.com/tphakala/birdnet-go/internal/conf"
	"github.com/tphakala/birdnet-go/internal/datastore"
	"github.com/tphakala/birdnet-go/internal/datastore/mocks"
	"github.com/tphakala/birdnet-go/internal/datastore/v2/entities"
	"github.com/tphakala/birdnet-go/internal/datastore/v2/repository"
	"github.com/tphakala/birdnet-go/internal/errors"
	"github.com/tphakala/birdnet-go/internal/observability/metrics"
	"github.com/tphakala/birdnet-go/internal/suncalc"
)

// ---------------------------------------------------------------------------
// Scripted provider that blocks inside FetchWeather so a test can republish
// settings precisely while the HTTP request is in flight.
// ---------------------------------------------------------------------------

// gateFetch scripts one FetchWeather invocation. When auto is false the call
// blocks on its release channel after signaling entered, giving the test a
// deterministic "request in flight" window; auto calls return immediately.
type gateFetch struct {
	data *WeatherData
	err  error
	auto bool
}

type gateProvider struct {
	mu      sync.Mutex
	calls   int
	seen    []*conf.Settings
	fetches []gateFetch
	entered []chan struct{}
	release []chan struct{}
}

func newGateProvider(fetches ...gateFetch) *gateProvider {
	p := &gateProvider{fetches: fetches}
	for range fetches {
		p.entered = append(p.entered, make(chan struct{}))
		p.release = append(p.release, make(chan struct{}))
	}
	return p
}

func (p *gateProvider) FetchWeather(ctx context.Context, settings *conf.Settings) (*WeatherData, error) {
	p.mu.Lock()
	i := p.calls
	p.calls++
	p.seen = append(p.seen, settings)
	if i >= len(p.fetches) {
		p.mu.Unlock()
		return nil, fmt.Errorf("gateProvider: unexpected fetch call %d", i)
	}
	f := p.fetches[i]
	entered, release := p.entered[i], p.release[i]
	p.mu.Unlock()

	// Signal that the request is in flight before blocking.
	close(entered)
	if f.auto {
		return f.data, f.err
	}
	select {
	case <-release:
		return f.data, f.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *gateProvider) callCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

func (p *gateProvider) settingsAt(t *testing.T, i int) *conf.Settings {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	require.Less(t, i, len(p.seen), "fetch call %d never happened", i)
	return p.seen[i]
}

// runFetch starts fetchAndSave in a goroutine and returns its result channel;
// tests combine it with the provider's entered/release channels from the main
// test goroutine (t.Fatalf is only safe to call there).
func runFetch(s *Service) <-chan error {
	ch := make(chan error, 1)
	go func() { ch <- s.fetchAndSave(context.Background()) }()
	return ch
}

func awaitResult(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("fetchAndSave did not return in time")
		return nil
	}
}

func awaitEntered(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("provider FetchWeather never entered the in-flight state")
	}
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const staleTestWait = 5 * time.Second

var (
	staleTimeA = time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC) // Helsinki era
	staleTimeB = time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC) // post-switch era
)

func settingsAt(t *testing.T, provider string, lat, lon float64, opts ...func(*conf.Settings)) *conf.Settings {
	t.Helper()
	o := append([]func(*conf.Settings){func(s *conf.Settings) {
		s.BirdNET.Latitude = lat
		s.BirdNET.Longitude = lon
	}}, opts...)
	return createTestSettings(t, provider, o...)
}

func helsinkiSettings(t *testing.T, provider string) *conf.Settings {
	t.Helper()
	return settingsAt(t, provider, 60.1699, 24.9384)
}

func helsinkiData(t *testing.T, at time.Time, temp float64) *WeatherData {
	t.Helper()
	return createTestWeatherData(t, func(d *WeatherData) {
		d.Time = at
		d.Location = Location{Latitude: 60.1699, Longitude: 24.9384, Country: "FI", City: "Helsinki"}
		d.Temperature.Current = temp
		d.WeatherMain = "Clouds"
		d.Description = "partly cloudy"
		d.Icon = "03"
		d.Precipitation = Precipitation{Amount: 0, Type: ""}
	})
}

func sydneyData(t *testing.T, at time.Time) *WeatherData {
	t.Helper()
	return createTestWeatherData(t, func(d *WeatherData) {
		d.Time = at
		d.Location = Location{Latitude: -33.8688, Longitude: 151.2093, Country: "AU", City: "Sydney"}
		d.Temperature = Temperature{Current: 25.0, FeelsLike: 26.4, Min: 21.0, Max: 29.0}
		d.Wind = Wind{Speed: 2.2, Deg: 350, Gust: 3.1}
		d.Precipitation = Precipitation{Amount: 2.5, Type: "rain"}
		d.Clouds = 5
		d.Visibility = 20000
		d.Pressure = 1020
		d.Humidity = 55
		d.WeatherMain = "Rain"
		d.Description = "light rain"
		d.Icon = "10"
	})
}

func aucklandData(t *testing.T, at time.Time) *WeatherData {
	t.Helper()
	return createTestWeatherData(t, func(d *WeatherData) {
		d.Time = at
		d.Location = Location{Latitude: -36.8485, Longitude: 174.7633, Country: "NZ", City: "Auckland"}
		d.Temperature.Current = 16.0
		d.Wind.Speed = 4.4
		d.Precipitation = Precipitation{Amount: 0.8, Type: "rain"}
		d.Clouds = 30
		d.Pressure = 1018
		d.Humidity = 70
		d.WeatherMain = "Rain"
		d.Description = "patchy rain"
		d.Icon = "09"
	})
}

func tampereData(t *testing.T, at time.Time) *WeatherData {
	t.Helper()
	return createTestWeatherData(t, func(d *WeatherData) {
		d.Time = at
		d.Location = Location{Latitude: 61.4978, Longitude: 23.7610, Country: "FI", City: "Tampere"}
		d.Temperature.Current = 18.0
		d.Icon = "04"
		d.Precipitation = Precipitation{Amount: 1.2, Type: "rain"}
	})
}

// newStaleGateService builds a Service the same way NewService wires one, but
// with an injected provider and sunCalc pre-reconciled for the settings
// coordinates, so tests exercise fetchAndSave/saveWeatherData without HTTP.
func newStaleGateService(t *testing.T, settings *conf.Settings, p Provider, name string, db datastore.Interface, m *metrics.WeatherMetrics) *Service {
	t.Helper()
	lat, lon := settings.BirdNET.Latitude, settings.BirdNET.Longitude
	return &Service{
		provider:          p,
		providerName:      name,
		db:                db,
		settings:          settings,
		metrics:           m,
		sunCalc:           suncalc.NewSunCalc(lat, lon),
		sunCalcLat:        lat,
		sunCalcLon:        lon,
		providerBaselined: true,
	}
}

// stashGlobalSettings arranges restoration of the process-wide settings
// snapshot, which these tests republish via conf.StoreSettings exactly as the
// settings API save path does. Not parallel-safe with other global-touching
// tests, so none of these call t.Parallel.
func stashGlobalSettings(t *testing.T) {
	t.Helper()
	prev := conf.GetSettings()
	t.Cleanup(func() { conf.StoreSettings(prev) })
}

// expectSavePair expects exactly one daily+hourly commit, assigning the daily
// row ID, with full matchers so tests can assert on the persisted snapshot.
func expectSavePair(t *testing.T, db *mocks.MockInterface, id uint,
	dailyMatch func(*datastore.DailyEvents) bool,
	hourlyMatch func(*datastore.HourlyWeather) bool,
) {
	t.Helper()
	if dailyMatch == nil {
		dailyMatch = func(*datastore.DailyEvents) bool { return true }
	}
	if hourlyMatch == nil {
		hourlyMatch = func(*datastore.HourlyWeather) bool { return true }
	}
	db.On("SaveDailyEvents", mock.MatchedBy(dailyMatch)).Run(func(args mock.Arguments) {
		args.Get(0).(*datastore.DailyEvents).ID = id
	}).Return(nil).Once()
	db.On("SaveHourlyWeather", mock.MatchedBy(hourlyMatch)).Return(nil).Once()
}

func clearBackoffWindow(s *Service) {
	s.backoff.mu.Lock()
	s.backoff.nextAllowedFetchTime = time.Time{}
	s.backoff.mu.Unlock()
}

// expectedSunrise is the sunrise unix seconds saveWeatherData must persist for
// dataAt when sunCalc was rebuilt for (lat, lon).
func expectedSunrise(t *testing.T, lat, lon float64, dataAt time.Time) int64 {
	t.Helper()
	times, err := suncalc.NewSunCalc(lat, lon).GetSunEventTimes(dataAt)
	require.NoError(t, err)
	return times.Sunrise.Unix()
}

// v2WeatherStore adapts the REAL v2 hourly-weather repository to the four
// datastore.Interface methods the weather service call chain uses
// (SaveDailyEvents/GetDailyEvents/SaveHourlyWeather/LatestHourlyWeather). The
// v2 schema is the one that persists precipitation amount/type; the deprecated
// legacy schema tags them gorm:"-". The embedded nil Interface covers the rest
// of the surface and would panic if any other method were ever called, keeping
// the adapter honest about which call graph these tests exercise.
type v2WeatherStore struct {
	datastore.Interface
	db      *gorm.DB
	weather repository.WeatherRepository
}

func newV2WeatherStore(t *testing.T) datastore.Interface {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "weather-stale.db")), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&entities.DailyEvents{}, &entities.HourlyWeather{}))
	t.Cleanup(func() {
		sqlDB, err := db.DB()
		assert.NoError(t, err)
		assert.NoError(t, sqlDB.Close())
	})
	return &v2WeatherStore{
		db:      db,
		weather: repository.NewWeatherRepository(db, nil, false, false),
	}
}

func (s *v2WeatherStore) SaveDailyEvents(de *datastore.DailyEvents) error {
	e := &entities.DailyEvents{
		ID: de.ID, Date: de.Date, Sunrise: de.Sunrise, Sunset: de.Sunset,
		Country: de.Country, CityName: de.CityName,
		MoonPhase: de.MoonPhase, MoonIllumination: de.MoonIllumination,
	}
	if err := s.weather.SaveDailyEvents(context.Background(), e); err != nil {
		return err
	}
	de.ID = e.ID // propagate upsert-assigned ID like the v2only datastore does
	return nil
}

func (s *v2WeatherStore) GetDailyEvents(date string) (datastore.DailyEvents, error) {
	e, err := s.weather.GetDailyEvents(context.Background(), date)
	if err != nil {
		return datastore.DailyEvents{}, err
	}
	return datastore.DailyEvents{
		ID: e.ID, Date: e.Date, Sunrise: e.Sunrise, Sunset: e.Sunset,
		Country: e.Country, CityName: e.CityName,
		MoonPhase: e.MoonPhase, MoonIllumination: e.MoonIllumination,
	}, nil
}

func (s *v2WeatherStore) SaveHourlyWeather(hw *datastore.HourlyWeather) error {
	if hw == nil {
		return errors.NewStd("hourly weather cannot be nil")
	}
	return s.weather.SaveHourlyWeather(context.Background(), &entities.HourlyWeather{
		DailyEventsID:     hw.DailyEventsID,
		Time:              hw.Time,
		Temperature:       hw.Temperature,
		FeelsLike:         hw.FeelsLike,
		TempMin:           hw.TempMin,
		TempMax:           hw.TempMax,
		Pressure:          hw.Pressure,
		Humidity:          hw.Humidity,
		Visibility:        hw.Visibility,
		WindSpeed:         hw.WindSpeed,
		WindDeg:           hw.WindDeg,
		WindGust:          hw.WindGust,
		Clouds:            hw.Clouds,
		Precipitation:     hw.Precipitation,
		PrecipitationType: hw.PrecipitationType,
		WeatherMain:       hw.WeatherMain,
		WeatherDesc:       hw.WeatherDesc,
		WeatherIcon:       hw.WeatherIcon,
	})
}

func (s *v2WeatherStore) LatestHourlyWeather() (*datastore.HourlyWeather, error) {
	e, err := s.weather.LatestHourlyWeather(context.Background())
	if err != nil {
		return nil, err
	}
	return &datastore.HourlyWeather{
		ID: e.ID, DailyEventsID: e.DailyEventsID, Time: e.Time,
		Temperature: e.Temperature, FeelsLike: e.FeelsLike, TempMin: e.TempMin, TempMax: e.TempMax,
		Pressure: e.Pressure, Humidity: e.Humidity, Visibility: e.Visibility,
		WindSpeed: e.WindSpeed, WindDeg: e.WindDeg, WindGust: e.WindGust,
		Clouds: e.Clouds,
		Precipitation:     e.Precipitation,
		PrecipitationType: e.PrecipitationType,
		WeatherMain:       e.WeatherMain,
		WeatherDesc:       e.WeatherDesc,
		WeatherIcon:       e.WeatherIcon,
	}, nil
}

// ---------------------------------------------------------------------------
// 1. Default fetch: unchanged configuration commits via the same snapshot.
// ---------------------------------------------------------------------------

func TestFetchConfigGate_DefaultFetchCommits(t *testing.T) {
	stashGlobalSettings(t)
	settings := helsinkiSettings(t, "yrno")
	conf.StoreSettings(settings)

	db := mocks.NewMockInterface(t)
	expectSavePair(t, db, 1,
		func(de *datastore.DailyEvents) bool {
			return de.CityName == "Helsinki" && de.Country == "FI"
		},
		func(hw *datastore.HourlyWeather) bool {
			return hw.DailyEventsID == 1 && hw.WeatherIcon == "03" && hw.Temperature == 10.0
		})

	p := newGateProvider(gateFetch{data: helsinkiData(t, staleTimeA, 10.0), auto: true})
	svc := newStaleGateService(t, settings, p, yrNoProviderName, db, nil)

	require.NoError(t, svc.fetchAndSave(t.Context()))
	db.AssertExpectations(t)
	assert.Equal(t, 1, p.callCount())
	assert.InDelta(t, 60.1699, p.settingsAt(t, 0).BirdNET.Latitude, 1e-9,
		"the provider must receive the captured Helsinki snapshot")
}

// ---------------------------------------------------------------------------
// 2. Late PROVIDER response: discarded, then new provider commits fully.
// ---------------------------------------------------------------------------

func TestFetchConfigGate_ProviderChangedDuringFetch(t *testing.T) {
	stashGlobalSettings(t)
	settingsA := helsinkiSettings(t, "yrno")
	conf.StoreSettings(settingsA)

	// No expectations at all on the datastore: a stale result must not produce
	// a single write; a testify mock fails on any unexpected call.
	db := mocks.NewMockInterface(t)

	dataA := helsinkiData(t, staleTimeA, 10.0)
	p := newGateProvider(
		gateFetch{data: dataA}, // blocks: yr.no/Helsinki request in flight
		gateFetch{data: sydneyData(t, staleTimeB), auto: true},
	)
	svc := newStaleGateService(t, settingsA, p, yrNoProviderName, db, nil)

	// Cycle 1: admin switches to OpenWeather/Sydney while the yr.no request is
	// in flight.
	resCh := runFetch(svc)
	awaitEntered(t, p.entered[0])
	conf.StoreSettings(settingsAt(t, "openweather", -33.8688, 151.2093))
	close(p.release[0])
	require.ErrorIs(t, awaitResult(t, resCh), ErrWeatherFetchStale,
		"the late yr.no result must be discarded with the stale sentinel")
	assert.Zero(t, svc.backoff.consecutiveFailures,
		"a discarded result must not move the failure backoff")
	db.AssertNotCalled(t, "SaveDailyEvents", mock.Anything)
	db.AssertNotCalled(t, "SaveHourlyWeather", mock.Anything)

	// Cycle 2 reaches the same post-reconcile state a background poll would
	// (reconcileConfig has now swapped the active provider to OpenWeather):
	// publish the scripted provider under the new name and let the new
	// configuration's fetch commit weather, precipitation, icon and sun times.
	svc.setProvider(p, openWeatherProviderName)
	wantSunrise := expectedSunrise(t, -33.8688, 151.2093, staleTimeB)
	expectSavePair(t, db, 7,
		func(de *datastore.DailyEvents) bool {
			return de.CityName == "Sydney" && de.Country == "AU" && de.Sunrise == wantSunrise
		},
		func(hw *datastore.HourlyWeather) bool {
			return hw.DailyEventsID == 7 &&
				hw.Temperature == 25.0 &&
				hw.Precipitation == 2.5 && hw.PrecipitationType == "rain" &&
				hw.WeatherMain == "Rain" && hw.WeatherIcon == "10"
		})

	require.NoError(t, svc.fetchAndSave(t.Context()))
	db.AssertExpectations(t)
	assert.Equal(t, 2, p.callCount())
	assert.InDelta(t, -33.8688, p.settingsAt(t, 1).BirdNET.Latitude, 1e-9,
		"the recovered fetch must run against the new location snapshot")
	assert.InDelta(t, 151.2093, p.settingsAt(t, 1).BirdNET.Longitude, 1e-9)
	assert.InDelta(t, -33.8688, svc.sunCalcLat, 1e-9, "sunCalc must track the new location")
}

// ---------------------------------------------------------------------------
// 3. Late LOCATION response: discarded, sun times follow the new location.
// ---------------------------------------------------------------------------

func TestFetchConfigGate_LocationChangedDuringFetch(t *testing.T) {
	stashGlobalSettings(t)
	settingsA := helsinkiSettings(t, "yrno")
	conf.StoreSettings(settingsA)

	db := mocks.NewMockInterface(t)
	p := newGateProvider(
		gateFetch{data: helsinkiData(t, staleTimeA, 10.0)}, // blocks at Helsinki
		gateFetch{data: sydneyData(t, staleTimeB), auto: true},
	)
	svc := newStaleGateService(t, settingsA, p, yrNoProviderName, db, nil)

	// Same provider, only the location moves to Sydney during the request.
	resCh := runFetch(svc)
	awaitEntered(t, p.entered[0])
	conf.StoreSettings(settingsAt(t, "yrno", -33.8688, 151.2093))
	close(p.release[0])
	require.ErrorIs(t, awaitResult(t, resCh), ErrWeatherFetchStale)
	assert.Equal(t, yrNoProviderName, svc.activeProviderName(),
		"a location-only change must not swap the provider")
	db.AssertNotCalled(t, "SaveDailyEvents", mock.Anything)
	db.AssertNotCalled(t, "SaveHourlyWeather", mock.Anything)

	wantSunrise := expectedSunrise(t, -33.8688, 151.2093, staleTimeB)
	expectSavePair(t, db, 9,
		func(de *datastore.DailyEvents) bool {
			return de.CityName == "Sydney" && de.Country == "AU" && de.Sunrise == wantSunrise
		},
		func(hw *datastore.HourlyWeather) bool {
			return hw.DailyEventsID == 9 && hw.Temperature == 25.0 && hw.WeatherIcon == "10" &&
				hw.Precipitation == 2.5 && hw.PrecipitationType == "rain"
		})

	require.NoError(t, svc.fetchAndSave(t.Context()))
	db.AssertExpectations(t)
	assert.InDelta(t, -33.8688, svc.sunCalcLat, 1e-9)
	assert.InDelta(t, 151.2093, svc.sunCalcLon, 1e-9)
}

// ---------------------------------------------------------------------------
// 4a. Timeout/network failure for the CURRENT config: last good observation is
// retained, an explainable error and backoff are reported, later cycle recovers.
// ---------------------------------------------------------------------------

func TestFetchConfigGate_NetworkFailureKeepsLastGoodAndRecovers(t *testing.T) {
	stashGlobalSettings(t)
	ds := newV2WeatherStore(t)
	settings := helsinkiSettings(t, "yrno")
	conf.StoreSettings(settings)

	p := newGateProvider(
		gateFetch{data: helsinkiData(t, staleTimeA, 10.0), auto: true},
		gateFetch{err: errors.NewStd(`Get "https://api.met.no/weatherapi/...": context deadline exceeded (Client.Timeout exceeded while awaiting headers)`), auto: true},
		gateFetch{data: helsinkiData(t, staleTimeA.Add(time.Hour), 11.0), auto: true},
	)
	svc := newStaleGateService(t, settings, p, yrNoProviderName, ds, nil)

	// Cycle 1 commits the last good observation.
	require.NoError(t, svc.fetchAndSave(t.Context()))
	last, err := ds.LatestHourlyWeather()
	require.NoError(t, err)
	assert.Equal(t, 10.0, last.Temperature)

	// Cycle 2 times out: error surfaces, backoff engages, the last good row is
	// still what API/dashboard readers see.
	err = svc.fetchAndSave(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "deadline exceeded",
		"the timeout must remain explainable through the wrapped error")
	ok, _ := svc.Status()
	assert.False(t, ok, "a timed-out fetch reports the service degraded/in backoff")
	assert.Positive(t, svc.backoff.consecutiveFailures)
	last, err = ds.LatestHourlyWeather()
	require.NoError(t, err)
	assert.Equal(t, 10.0, last.Temperature, "last good observation must be retained")

	// After the backoff window elapses (next poll cycle), success recovers and
	// commits fresh data.
	clearBackoffWindow(svc)
	require.NoError(t, svc.fetchAndSave(t.Context()))
	last, err = ds.LatestHourlyWeather()
	require.NoError(t, err)
	assert.Equal(t, 11.0, last.Temperature, "a later cycle must recover with fresh data")
	ok, _ = svc.Status()
	assert.True(t, ok, "successful recovery restores healthy status")
}

// 4b. A late ERROR result after a provider switch is discarded without backoff
// or telemetry; the new provider's own failure preserves last-good and its
// subsequent success commits the new snapshot.
func TestFetchConfigGate_StaleErrorDroppedThenNewConfigRecovers(t *testing.T) {
	stashGlobalSettings(t)
	ds := newV2WeatherStore(t)
	settingsA := helsinkiSettings(t, "yrno")
	conf.StoreSettings(settingsA)

	p := newGateProvider(
		gateFetch{data: helsinkiData(t, staleTimeA, 10.0), auto: true}, // last good
		gateFetch{err: errors.NewStd("context deadline exceeded")},     // blocks; switched mid-flight
		gateFetch{err: errors.NewStd("dial tcp: i/o timeout"), auto: true},
		gateFetch{data: sydneyData(t, staleTimeB), auto: true},
	)
	svc := newStaleGateService(t, settingsA, p, yrNoProviderName, ds, nil)
	require.NoError(t, svc.fetchAndSave(t.Context()))

	// Switch during the in-flight timeout of the OLD provider: the error must
	// be discarded as stale, not recorded against the new generation's backoff.
	resCh := runFetch(svc)
	awaitEntered(t, p.entered[1])
	conf.StoreSettings(settingsAt(t, "openweather", -33.8688, 151.2093))
	close(p.release[1])
	require.ErrorIs(t, awaitResult(t, resCh), ErrWeatherFetchStale)
	assert.Zero(t, svc.backoff.consecutiveFailures,
		"the old generation's timeout must not start the new generation's backoff")

	last, err := ds.LatestHourlyWeather()
	require.NoError(t, err)
	assert.Equal(t, 10.0, last.Temperature, "last good observation retained through the switch")

	// New provider fails on its own merits: backoff now engages, last good stays.
	svc.setProvider(p, openWeatherProviderName)
	require.Error(t, svc.fetchAndSave(t.Context()))
	assert.Equal(t, 1, svc.backoff.consecutiveFailures)
	last, err = ds.LatestHourlyWeather()
	require.NoError(t, err)
	assert.Equal(t, 10.0, last.Temperature)

	// Recovery cycle commits Sydney (weather, precipitation, icon).
	clearBackoffWindow(svc)
	require.NoError(t, svc.fetchAndSave(t.Context()))
	last, err = ds.LatestHourlyWeather()
	require.NoError(t, err)
	assert.Equal(t, 25.0, last.Temperature)
	assert.Equal(t, "10", last.WeatherIcon)
	assert.Equal(t, 2.5, last.Precipitation)
	assert.Equal(t, "rain", last.PrecipitationType)
}

// ---------------------------------------------------------------------------
// 5. Duplicate on-demand refreshes: serialized; the old result never commits,
// the queued refresh commits the new snapshot.
// ---------------------------------------------------------------------------

func TestPoll_DuplicateRefreshDuringConfigChange(t *testing.T) {
	stashGlobalSettings(t)
	settingsA := helsinkiSettings(t, "yrno")
	conf.StoreSettings(settingsA)

	db := mocks.NewMockInterface(t)
	p := newGateProvider(
		gateFetch{data: helsinkiData(t, staleTimeA, 10.0)}, // first refresh blocks
		gateFetch{data: tampereData(t, staleTimeB), auto: true},
	)
	svc := newStaleGateService(t, settingsA, p, yrNoProviderName, db, nil)

	// The single committed observation must already be expected before the
	// refreshes run; the stale first cycle may produce zero writes.
	expectSavePair(t, db, 1,
		func(de *datastore.DailyEvents) bool { return de.CityName == "Tampere" && de.Country == "FI" },
		func(hw *datastore.HourlyWeather) bool {
			return hw.Temperature == 18.0 && hw.WeatherIcon == "04" && hw.Precipitation == 1.2
		})

	// Two refresh clicks arrive concurrently; the second queues on fetchMu
	// (which goroutine wins the lock is deliberately not assumed).
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i := range 2 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = svc.Poll(context.Background())
		}(i)
	}

	awaitEntered(t, p.entered[0])
	// Location-only change keeps the yr.no provider; reconcile on the queued
	// refresh rebuilds sunCalc for Tampere.
	conf.StoreSettings(settingsAt(t, "yrno", 61.4978, 23.7610))
	close(p.release[0])
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(staleTestWait):
		t.Fatal("duplicate Poll calls did not both return")
	}

	var staleCount, okCount int
	for _, e := range errs {
		switch {
		case errors.Is(e, ErrWeatherFetchStale):
			staleCount++
		case e == nil:
			okCount++
		default:
			t.Fatalf("unexpected Poll error: %v", e)
		}
	}
	assert.Equal(t, 1, staleCount, "one refresh must be told its result was discarded")
	assert.Equal(t, 1, okCount, "the queued refresh must complete against the new snapshot")
	assert.Equal(t, 2, p.callCount(), "exactly two serialized fetches")
	assert.InDelta(t, 61.4978, p.settingsAt(t, 1).BirdNET.Latitude, 1e-9,
		"the committed fetch must carry the new location snapshot")
	db.AssertExpectations(t)
}

// Duplicate refreshes without a config change both commit normally (regression
// guard that the gate does not drop same-snapshot results).
func TestPoll_DuplicateRefreshUnchangedConfig(t *testing.T) {
	stashGlobalSettings(t)
	settings := helsinkiSettings(t, "yrno")
	conf.StoreSettings(settings)

	db := mocks.NewMockInterface(t)
	p := newGateProvider(
		gateFetch{data: helsinkiData(t, staleTimeA, 10.0), auto: true},
		gateFetch{data: helsinkiData(t, staleTimeA.Add(time.Hour), 11.0), auto: true},
	)
	svc := newStaleGateService(t, settings, p, yrNoProviderName, db, nil)

	db.On("SaveDailyEvents", mock.Anything).Run(func(args mock.Arguments) {
		args.Get(0).(*datastore.DailyEvents).ID = 1
	}).Return(nil)
	db.On("SaveHourlyWeather", mock.Anything).Return(nil)

	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			assert.NoError(t, svc.Poll(context.Background()))
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(staleTestWait):
		t.Fatal("duplicate Poll calls did not both return")
	}
	assert.Equal(t, 2, p.callCount())
	db.AssertNumberOfCalls(t, "SaveHourlyWeather", 2)
	db.AssertNumberOfCalls(t, "SaveDailyEvents", 2)
}

// ---------------------------------------------------------------------------
// 6. Process restart: a fresh Service over the same database fetches only the
// configuration it started with, arms the gate on its first request, and its
// first success replaces what the API/dashboard read as "latest".
// ---------------------------------------------------------------------------

func TestFetchConfigGate_ProcessRestartCommitsCurrentConfig(t *testing.T) {
	stashGlobalSettings(t)

	// --- Previous process: Helsinki provider, one committed observation. ---
	ds := newV2WeatherStore(t)
	settingsA := helsinkiSettings(t, "yrno")
	conf.StoreSettings(settingsA)
	oldSvc := newStaleGateService(t, settingsA,
		newGateProvider(gateFetch{data: helsinkiData(t, staleTimeA, 10.0), auto: true}),
		yrNoProviderName, ds, nil)
	require.NoError(t, oldSvc.fetchAndSave(t.Context()))
	last, err := ds.LatestHourlyWeather()
	require.NoError(t, err)
	assert.Equal(t, 10.0, last.Temperature)
	dateA := staleTimeA.In(time.Local).Format(time.DateOnly)
	dailyA, err := ds.GetDailyEvents(dateA)
	require.NoError(t, err)
	assert.Equal(t, "Helsinki", dailyA.CityName)

	// Config file on disk has moved to yr.no/Sydney before the restart. The new
	// process must begin its polling life with that snapshot only.
	settingsB := settingsAt(t, "yrno", -33.8688, 151.2093)
	conf.StoreSettings(settingsB)

	// --- New process: a fresh Service (same construction as NewService). Its
	// first request is still in flight when the admin saves yet another move to
	// Auckland; the generation gate is armed from this very first request, so
	// the late Sydney result is dropped.
	p := newGateProvider(
		gateFetch{data: sydneyData(t, staleTimeB)}, // blocks
		gateFetch{data: aucklandData(t, staleTimeB.Add(time.Hour)), auto: true},
	)
	newSvc := newStaleGateService(t, settingsB, p, yrNoProviderName, ds, nil)

	resCh := runFetch(newSvc)
	awaitEntered(t, p.entered[0])
	// The restarted service never fetched the old Helsinki configuration.
	assert.InDelta(t, -33.8688, p.settingsAt(t, 0).BirdNET.Latitude, 1e-9)
	conf.StoreSettings(settingsAt(t, "yrno", -36.8485, 174.7633))
	close(p.release[0])
	require.ErrorIs(t, awaitResult(t, resCh), ErrWeatherFetchStale)

	// The stale Sydney result is not visible; latest is still the previous
	// process's last good row.
	last, err = ds.LatestHourlyWeather()
	require.NoError(t, err)
	assert.Equal(t, 10.0, last.Temperature, "late result must not be visible in the restarted process")

	// First successful fetch of the CURRENT (Auckland) snapshot becomes latest:
	// weather, precipitation and icon all come from that snapshot.
	require.NoError(t, newSvc.fetchAndSave(t.Context()))
	last, err = ds.LatestHourlyWeather()
	require.NoError(t, err)
	assert.Equal(t, 16.0, last.Temperature)
	assert.Equal(t, "09", last.WeatherIcon)
	assert.Equal(t, 0.8, last.Precipitation)
	assert.Equal(t, "rain", last.PrecipitationType)

	// Sun times for that day are computed for Auckland (same provider, location
	// rebuilt at cycle start), while Helsinki history stays untouched.
	aucklandDate := staleTimeB.Add(time.Hour).In(time.Local).Format(time.DateOnly)
	dailyC, err := ds.GetDailyEvents(aucklandDate)
	require.NoError(t, err)
	assert.Equal(t, "Auckland", dailyC.CityName)
	assert.Equal(t, "NZ", dailyC.Country)
	assert.Equal(t, expectedSunrise(t, -36.8485, 174.7633, staleTimeB.Add(time.Hour)), dailyC.Sunrise)

	dailyAAfter, err := ds.GetDailyEvents(dateA)
	require.NoError(t, err)
	assert.Equal(t, "Helsinki", dailyAAfter.CityName)
	assert.Equal(t, dailyA.Sunrise, dailyAAfter.Sunrise,
		"the new configuration must not rewrite the old location's daily row")
}

// ---------------------------------------------------------------------------
// Telemetry: a stale result must not touch gauges or fetch counters; the new
// snapshot's success is what telemetry ends up reporting.
// ---------------------------------------------------------------------------

func gatherFamily(t *testing.T, reg prometheus.Gatherer, name string) *dto.MetricFamily {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() == name {
			return f
		}
	}
	require.Failf(t, "metric family missing", "name=%s", name)
	return nil
}

// gatherFamilyOK returns the family and false when no label series has been
// sampled yet (a CounterVec with zero touched series is absent from Gather);
// counterValue(nil, ...) reads as 0, which is exactly the "never booked" value
// the telemetry test asserts.
func gatherFamilyOK(t *testing.T, reg prometheus.Gatherer, name string) (*dto.MetricFamily, bool) {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() == name {
			return f, true
		}
	}
	return nil, false
}

func labelValue(ms []*dto.LabelPair, name string) string {
	for _, lp := range ms {
		if lp.GetName() == name {
			return lp.GetValue()
		}
	}
	return ""
}

func counterValue(f *dto.MetricFamily, labels map[string]string) float64 {
	for _, m := range f.GetMetric() {
		match := true
		for k, v := range labels {
			if labelValue(m.GetLabel(), k) != v {
				match = false
				break
			}
		}
		if match {
			return m.GetCounter().GetValue()
		}
	}
	return 0
}

func gaugeValue(f *dto.MetricFamily) float64 {
	for _, m := range f.GetMetric() {
		return m.GetGauge().GetValue()
	}
	return 0
}

func TestFetchConfigGate_StaleResultDoesNotTouchTelemetry(t *testing.T) {
	stashGlobalSettings(t)
	settingsA := helsinkiSettings(t, "yrno")
	conf.StoreSettings(settingsA)

	reg := prometheus.NewRegistry()
	wm, err := metrics.NewWeatherMetrics(reg)
	require.NoError(t, err)

	db := mocks.NewMockInterface(t)
	p := newGateProvider(
		gateFetch{data: helsinkiData(t, staleTimeA, 10.0), auto: true},
		gateFetch{data: helsinkiData(t, staleTimeA.Add(time.Hour), 99.0)}, // stale poison
		gateFetch{data: sydneyData(t, staleTimeB), auto: true},
	)
	svc := newStaleGateService(t, settingsA, p, yrNoProviderName, db, wm)

	// Both successful cycles commit (2 pairs); the stale cycle commits nothing.
	db.On("SaveDailyEvents", mock.Anything).Run(func(args mock.Arguments) {
		args.Get(0).(*datastore.DailyEvents).ID = 1
	}).Return(nil)
	db.On("SaveHourlyWeather", mock.Anything).Return(nil)

	// First success sets gauges/counters for yr.no.
	require.NoError(t, svc.fetchAndSave(t.Context()))
	tempGauge := func() float64 {
		return gaugeValue(gatherFamily(t, reg, "weather_temperature_celsius"))
	}
	assert.Equal(t, 10.0, tempGauge())

	// Switch to OpenWeather/Sydney during a response that would report 99 C.
	resCh := runFetch(svc)
	awaitEntered(t, p.entered[1])
	conf.StoreSettings(settingsAt(t, "openweather", -33.8688, 151.2093))
	close(p.release[1])
	require.ErrorIs(t, awaitResult(t, resCh), ErrWeatherFetchStale)

	// Gauges keep the last good value; no fetch/error counters are booked for
	// the discarded result.
	assert.Equal(t, 10.0, tempGauge(), "stale result must not move the current-weather gauges")
	fetches := gatherFamily(t, reg, "weather_fetches_total")
	assert.Equal(t, 1.0, counterValue(fetches,
		map[string]string{"provider": yrNoProviderName, "status": "success"}))
	assert.Equal(t, 0.0, counterValue(fetches,
		map[string]string{"provider": yrNoProviderName, "status": "error"}))
	assert.Equal(t, 0.0, counterValue(fetches,
		map[string]string{"provider": openWeatherProviderName, "status": "success"}))
	fetchErrors, hasFetchErrors := gatherFamilyOK(t, reg, "weather_fetch_errors_total")
	assert.False(t, hasFetchErrors, "no fetch-error series may be sampled for a discarded result")
	assert.Equal(t, 0.0, counterValue(fetchErrors,
		map[string]string{"provider": yrNoProviderName, "error_type": "fetch_error"}))

	// New snapshot success updates gauges and books its telemetry exactly once.
	svc.setProvider(p, openWeatherProviderName)
	require.NoError(t, svc.fetchAndSave(t.Context()))
	assert.Equal(t, 25.0, tempGauge())
	fetches = gatherFamily(t, reg, "weather_fetches_total")
	assert.Equal(t, 1.0, counterValue(fetches,
		map[string]string{"provider": openWeatherProviderName, "status": "success"}))
	assert.Equal(t, 1.0, counterValue(fetches,
		map[string]string{"provider": yrNoProviderName, "status": "success"}))
	db.AssertNumberOfCalls(t, "SaveHourlyWeather", 2) // the 99 C stale observation was never saved
}
