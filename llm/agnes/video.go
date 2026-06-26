package agnes

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
)

// Agnes video defaults, per the API docs' recommended standard generation.
const (
	defVideoWidth     = 1152
	defVideoHeight    = 768
	defVideoNumFrames = 121
	defVideoFrameRate = 24.0
)

// VideoModel is an Agnes video-generation model (e.g. "agnes-video-v2.0"). It is
// asynchronous: GenerateVideo submits a job, then polls until it completes. It
// implements llm.ResumableVideoModel, so a caller can persist the JobID and
// reattach after a restart instead of paying to regenerate.
type VideoModel struct {
	model string
	cfg   config
}

// Video constructs an Agnes video model.
func Video(model, apiKey string, opts ...Option) *VideoModel {
	return &VideoModel{model: model, cfg: newConfig(apiKey, opts...)}
}

func (m *VideoModel) Name() string { return m.model }

// GenerateVideo implements llm.VideoModel: it submits a job, emits the initial
// queued progress, then polls (every cfg.poll) until the job reaches a terminal
// state. Context cancellation stops polling.
func (m *VideoModel) GenerateVideo(ctx context.Context, req *llm.VideoRequest) iter.Seq2[*llm.VideoProgress, error] {
	return func(yield func(*llm.VideoProgress, error) bool) {
		created, err := m.submit(ctx, req)
		if err != nil {
			yield(nil, err)
			return
		}
		first := &llm.VideoProgress{
			JobID:   created.VideoID,
			Status:  mapVideoStatus(created.Status),
			Percent: created.Progress,
		}
		if !yield(first, nil) {
			return
		}
		if isTerminal(first.Status) {
			return
		}
		m.pollUntilDone(ctx, created.VideoID, yield)
	}
}

// ResumeVideo implements llm.ResumableVideoModel: it polls an existing job id.
func (m *VideoModel) ResumeVideo(ctx context.Context, jobID string) iter.Seq2[*llm.VideoProgress, error] {
	return func(yield func(*llm.VideoProgress, error) bool) {
		m.pollUntilDone(ctx, jobID, yield)
	}
}

func (m *VideoModel) pollUntilDone(ctx context.Context, videoID string, yield func(*llm.VideoProgress, error) bool) {
	for {
		select {
		case <-ctx.Done():
			yield(nil, ctx.Err())
			return
		case <-time.After(m.cfg.poll):
		}
		q, err := m.query(ctx, videoID)
		if err != nil {
			yield(nil, err)
			return
		}
		p := toProgress(videoID, q)
		if !yield(p, nil) {
			return
		}
		if isTerminal(p.Status) {
			return
		}
	}
}

// --- submit -----------------------------------------------------------------

type videoCreateBody struct {
	Model          string         `json:"model"`
	Prompt         string         `json:"prompt"`
	Image          string         `json:"image,omitempty"` // single image-to-video
	Width          int            `json:"width,omitempty"`
	Height         int            `json:"height,omitempty"`
	NumFrames      int            `json:"num_frames,omitempty"`
	FrameRate      float64        `json:"frame_rate,omitempty"`
	Seed           *int64         `json:"seed,omitempty"`
	NegativePrompt string         `json:"negative_prompt,omitempty"`
	ExtraBody      map[string]any `json:"extra_body,omitempty"` // multi-image / keyframes
}

func (m *VideoModel) buildCreateBody(req *llm.VideoRequest) videoCreateBody {
	o := req.Options
	b := videoCreateBody{
		Model:          pick(o.Model, m.model),
		Prompt:         req.Prompt,
		Width:          orInt(o.Width, defVideoWidth),
		Height:         orInt(o.Height, defVideoHeight),
		NumFrames:      orInt(o.NumFrames, defVideoNumFrames),
		FrameRate:      orFloat(o.FrameRate, defVideoFrameRate),
		Seed:           o.Seed,
		NegativePrompt: o.NegativePrompt,
	}
	// Route input images per the docs: a single image is top-level "image"
	// (image-to-video); several images, or keyframe mode, go in extra_body.
	refs := imageRefs(req.InputImages)
	switch {
	case o.Mode == "keyframes":
		extra := map[string]any{"mode": "keyframes"}
		if len(refs) > 0 {
			extra["image"] = refs
		}
		b.ExtraBody = extra
	case len(refs) > 1:
		b.ExtraBody = map[string]any{"image": refs}
	case len(refs) == 1:
		b.Image = refs[0]
	}
	return b
}

type videoCreateResp struct {
	VideoID  string `json:"video_id"`
	TaskID   string `json:"task_id"`
	Status   string `json:"status"`
	Progress int    `json:"progress"`
}

func (m *VideoModel) submit(ctx context.Context, req *llm.VideoRequest) (*videoCreateResp, error) {
	body, err := json.Marshal(m.buildCreateBody(req))
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, m.cfg.baseURL+"/v1/videos", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("content-type", "application/json")
	httpReq.Header.Set("authorization", "Bearer "+m.cfg.apiKey)

	resp, err := m.cfg.http.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("agnes video: create status %d: %s", resp.StatusCode, data)
	}
	var cr videoCreateResp
	if err := json.Unmarshal(data, &cr); err != nil {
		return nil, fmt.Errorf("agnes video: decode create: %w", err)
	}
	if cr.VideoID == "" {
		return nil, fmt.Errorf("agnes video: create returned no video_id: %s", data)
	}
	return &cr, nil
}

// --- query ------------------------------------------------------------------

type videoQueryResp struct {
	VideoID            string          `json:"video_id"`
	Status             string          `json:"status"`
	Progress           int             `json:"progress"`
	Seconds            string          `json:"seconds"`
	Size               string          `json:"size"`
	RemixedFromVideoID string          `json:"remixed_from_video_id"` // the final video URL
	Error              json.RawMessage `json:"error"`
}

func (m *VideoModel) query(ctx context.Context, videoID string) (*videoQueryResp, error) {
	// Recommended query endpoint: GET /agnesapi?video_id=...&model_name=...
	u := m.cfg.baseURL + "/agnesapi?video_id=" + url.QueryEscape(videoID) +
		"&model_name=" + url.QueryEscape(m.model)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("authorization", "Bearer "+m.cfg.apiKey)

	resp, err := m.cfg.http.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("agnes video: query status %d: %s", resp.StatusCode, data)
	}
	var qr videoQueryResp
	if err := json.Unmarshal(data, &qr); err != nil {
		return nil, fmt.Errorf("agnes video: decode query: %w", err)
	}
	return &qr, nil
}

// --- mapping ----------------------------------------------------------------

func toProgress(videoID string, q *videoQueryResp) *llm.VideoProgress {
	p := &llm.VideoProgress{
		JobID:   videoID,
		Status:  mapVideoStatus(q.Status),
		Percent: q.Progress,
	}
	switch p.Status {
	case llm.JobSucceeded:
		p.Video = &core.Video{
			MIME:       "video/mp4",
			URL:        q.RemixedFromVideoID,
			DurationMs: secondsToMs(q.Seconds),
		}
	case llm.JobFailed:
		if len(q.Error) > 0 && string(q.Error) != "null" {
			p.Err = string(q.Error)
		} else {
			p.Err = "video generation failed"
		}
	}
	return p
}

func mapVideoStatus(s string) llm.JobStatus {
	switch s {
	case "completed":
		return llm.JobSucceeded
	case "failed":
		return llm.JobFailed
	case "in_progress":
		return llm.JobRunning
	default: // "queued" and anything unknown
		return llm.JobQueued
	}
}

func isTerminal(s llm.JobStatus) bool {
	return s == llm.JobSucceeded || s == llm.JobFailed
}

func secondsToMs(s string) int {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	return int(f * 1000)
}

func orInt(v, def int) int {
	if v > 0 {
		return v
	}
	return def
}

func orFloat(v, def float64) float64 {
	if v > 0 {
		return v
	}
	return def
}

var (
	_ llm.VideoModel          = (*VideoModel)(nil)
	_ llm.ResumableVideoModel = (*VideoModel)(nil)
)
