package lookup

import (
	"context"

	ua "github.com/mileusna/useragent"
)

// UserAgent is a lookup table that parses user-agent strings into structured fields.
// Pure computation — no I/O, no config, no caching needed.
type UserAgent struct{}

var _ LookupTable = (*UserAgent)(nil)

// NewUserAgent creates a user-agent parser lookup table.
func NewUserAgent() *UserAgent { return &UserAgent{} }

// Parameters returns the single input parameter name.
func (u *UserAgent) Parameters() []string { return []string{"value"} }

// Suffixes returns the output field names.
func (u *UserAgent) Suffixes() []string {
	return []string{"browser", "browser_version", "os", "os_version", "device", "device_type"}
}

// LookupValues parses a user-agent string and returns structured fields.
func (u *UserAgent) LookupValues(_ context.Context, values map[string]string) map[string]string {
	raw := values["value"]
	if raw == "" {
		return nil
	}

	parsed := ua.Parse(raw)
	if parsed.Name == "" && parsed.OS == "" {
		return nil
	}

	result := make(map[string]string, 6)
	if parsed.Name != "" {
		result["browser"] = parsed.Name
	}
	if parsed.Version != "" {
		result["browser_version"] = parsed.Version
	}
	if parsed.OS != "" {
		result["os"] = parsed.OS
	}
	if parsed.OSVersion != "" {
		result["os_version"] = parsed.OSVersion
	}
	if parsed.Device != "" {
		result["device"] = parsed.Device
	}

	switch {
	case parsed.Bot:
		result["device_type"] = "bot"
	case parsed.Tablet:
		result["device_type"] = "tablet"
	case parsed.Mobile:
		result["device_type"] = "mobile"
	case parsed.Desktop:
		result["device_type"] = "desktop"
	}

	return result
}
