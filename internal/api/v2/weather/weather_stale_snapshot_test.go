package weather

// Read-path regression companion to the weather service's provider/location
// commit gate (internal/weather fetchConfigSnapshot tests). The service
// deterministically discards a response that arrives after a provider/location
// change mid-flight, so such an observation never reaches the hourly-weather
// repository. These tests pin the other half of the invariant: /weather/latest
// and the daily pairing expose exactly one committed snapshot — hourly
// weather/precipitation/icon together with the daily sun times/city selected
// by the hourly row's own date — with no fields from an older configuration
// mixed in.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tphakala/birdnet-go/internal/datastore"
)

// TestGetLatestWeather_ReflectsSingleCommittedSnapshot verifies the dashboard
// data source returns the new configuration's committed observation as one
// unit: hourly temperature, precipitation and icon plus the Sydney daily row
// (city and sunrise/sunset). When the service drops a late old-provider
// response, this is the only shape the repository can contain, so nothing
// older is observable through the API.
func TestGetLatestWeather_ReflectsSingleCommittedSnapshot(t *testing.T) {
	e, mockDS, controller := setupWeatherTestEnvironment(t)

	// A row exactly as the post-switch service would commit it (Sydney,
	// 2026-06-21 12:00 UTC, rain, icon "10").
	obsTime := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	localDate := obsTime.In(time.Local).Format("2006-01-02")
	sunrise := time.Date(2026, 6, 21, 7, 0, 0, 0, time.UTC).Unix()
	sunset := time.Date(2026, 6, 21, 16, 59, 0, 0, time.UTC).Unix()

	mockDS.On("LatestHourlyWeather").Return(&datastore.HourlyWeather{
		Time:              obsTime,
		Temperature:       25.0,
		FeelsLike:         26.4,
		Pressure:          1020,
		Humidity:          55,
		Visibility:        20000,
		WindSpeed:         2.2,
		Clouds:            5,
		Precipitation:     2.5,
		PrecipitationType: "rain",
		WeatherMain:       "Rain",
		WeatherDesc:       "light rain",
		WeatherIcon:       "10",
	}, nil)
	mockDS.On("GetDailyEvents", localDate).Return(datastore.DailyEvents{
		Date:     localDate,
		Sunrise:  sunrise,
		Sunset:   sunset,
		Country:  "AU",
		CityName: "Sydney",
	}, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/v2/weather/latest", http.NoBody)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetPath("/api/v2/weather/latest")

	require.NoError(t, controller.GetLatestWeather(c))
	assert.Equal(t, http.StatusOK, rec.Code)

	var response struct {
		Daily  *dailyWeatherResponse  `json:"daily"`
		Hourly hourlyWeatherResponse  `json:"hourly"`
		Moon   map[string]any         `json:"moon"`
		Time   string                 `json:"timestamp"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))

	// Hourly half of the snapshot: weather + precipitation + icon.
	assert.InDelta(t, 25.0, response.Hourly.Temperature, 0.01)
	assert.InDelta(t, 2.5, response.Hourly.Precipitation, 0.01)
	assert.Equal(t, "rain", response.Hourly.PrecipitationType)
	assert.Equal(t, "10", response.Hourly.WeatherIcon)
	assert.Equal(t, "Rain", response.Hourly.WeatherMain)
	// No leftover old-observation values (the Helsinki row used 0 mm / icon 03).
	assert.NotEqual(t, "03", response.Hourly.WeatherIcon)

	// Daily half: sun times/city are paired with the hourly row's own date.
	require.NotNil(t, response.Daily)
	assert.Equal(t, localDate, response.Daily.Date)
	assert.Equal(t, "Sydney", response.Daily.CityName)
	assert.Equal(t, "AU", response.Daily.Country)
	assert.True(t, time.Unix(sunrise, 0).Equal(response.Daily.Sunrise), "sunrise must come from the same committed snapshot")
	assert.True(t, time.Unix(sunset, 0).Equal(response.Daily.Sunset))
	assert.Equal(t, obsTime.Format(time.RFC3339), response.Time)

	mockDS.AssertExpectations(t)
}
