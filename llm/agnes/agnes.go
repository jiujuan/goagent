// Package agnes implements goagent's image- and video-generation capability
// interfaces (llm.ImageModel, llm.VideoModel) against the Agnes AI gateway. The
// chat model is OpenAI-compatible and lives in llm/openaicompat
// (openaicompat.Agnes); image and video use Agnes-specific endpoints and so are
// implemented here, isolated like every other provider.
//
// The request bodies follow the Agnes API docs literally, including the nested
// "extra_body" object the gateway expects for image-to-image inputs and video
// keyframe modes (it is sent as a real nested object, not flattened).
package agnes

import (
	"encoding/base64"
	"net/http"
	"time"

	"github.com/jiujuan/goagent/core"
)

// DefaultBaseURL is the Agnes apihub host root. Image, video-create and
// video-query endpoints hang off it (the query endpoint /agnesapi is not under
// /v1, which is why constructors take the host root rather than a /v1 base).
const DefaultBaseURL = "https://apihub.agnes-ai.com"

// config is the shared transport configuration for the Agnes models.
type config struct {
	baseURL string
	apiKey  string
	http    *http.Client
	poll    time.Duration // video poll interval
}

func newConfig(apiKey string, opts ...Option) config {
	c := config{
		baseURL: DefaultBaseURL,
		apiKey:  apiKey,
		http:    &http.Client{Timeout: 6 * time.Minute},
		poll:    5 * time.Second,
	}
	for _, o := range opts {
		o(&c)
	}
	return c
}

// Option configures an Agnes model.
type Option func(*config)

// WithBaseURL overrides the gateway host root (e.g. a regional deployment).
func WithBaseURL(u string) Option { return func(c *config) { c.baseURL = u } }

// WithHTTPClient sets a custom HTTP client (timeouts for video should be
// generous; the default is 6 minutes).
func WithHTTPClient(h *http.Client) Option { return func(c *config) { c.http = h } }

// WithPollInterval sets how often video jobs are polled (default 5s, per the
// Agnes docs' recommendation).
func WithPollInterval(d time.Duration) Option {
	return func(c *config) {
		if d > 0 {
			c.poll = d
		}
	}
}

// imageRef renders a core.Image as the string Agnes expects: a public URL when
// present, otherwise a Data URI built from the inline bytes. Returns "" when the
// image carries neither.
func imageRef(img core.Image) string {
	if img.URL != "" {
		return img.URL
	}
	if len(img.Data) > 0 {
		mime := img.MIME
		if mime == "" {
			mime = "image/png"
		}
		return "data:" + mime + ";base64," + base64.StdEncoding.EncodeToString(img.Data)
	}
	return ""
}

func imageRefs(imgs []core.Image) []string {
	out := make([]string, 0, len(imgs))
	for _, img := range imgs {
		if ref := imageRef(img); ref != "" {
			out = append(out, ref)
		}
	}
	return out
}

func pick(a, b string) string {
	if a != "" {
		return a
	}
	return b
}
