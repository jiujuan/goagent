package agnes

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
)

func TestImageGenerateTextToImage(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/images/generations" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		if auth := r.Header.Get("authorization"); auth != "Bearer KEY" {
			t.Errorf("missing bearer auth, got %q", auth)
		}
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &gotBody)
		io.WriteString(w, `{"created":1,"data":[{"url":"https://cdn/x.png"}]}`)
	}))
	defer srv.Close()

	m := Image("agnes-image-2.1-flash", "KEY", WithBaseURL(srv.URL))
	req := &llm.ImageRequest{Prompt: "a cat"}
	req.Options.Apply(llm.WithImageSize("512x512"))

	var got *llm.ImageResponse
	for r, err := range m.GenerateImage(context.Background(), req) {
		if err != nil {
			t.Fatalf("GenerateImage: %v", err)
		}
		got = r
	}
	if got == nil || len(got.Images) != 1 || got.Images[0].URL != "https://cdn/x.png" {
		t.Fatalf("unexpected images: %+v", got)
	}
	// Top-level model/prompt/size; response_format lives inside extra_body.
	if gotBody["size"] != "512x512" || gotBody["prompt"] != "a cat" {
		t.Errorf("bad top-level fields: %v", gotBody)
	}
	extra, ok := gotBody["extra_body"].(map[string]any)
	if !ok || extra["response_format"] != "url" {
		t.Errorf("extra_body.response_format not url: %v", gotBody["extra_body"])
	}
	if _, present := gotBody["response_format"]; present {
		t.Errorf("response_format must NOT be top-level (Agnes returns 400)")
	}
}

func TestImageGenerateImageToImageBase64(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &gotBody)
		// 1x1 png base64 (not validated for content, just decoded)
		io.WriteString(w, `{"created":1,"data":[{"b64_json":"aGVsbG8="}]}`)
	}))
	defer srv.Close()

	m := Image("agnes-image-2.1-flash", "KEY", WithBaseURL(srv.URL))
	req := &llm.ImageRequest{
		Prompt:      "make it blue",
		InputImages: []core.Image{{URL: "https://in/a.png"}},
	}
	var got *llm.ImageResponse
	for r, err := range m.GenerateImage(context.Background(), req) {
		if err != nil {
			t.Fatalf("GenerateImage: %v", err)
		}
		got = r
	}
	if got == nil || len(got.Images) != 1 || string(got.Images[0].Data) != "hello" {
		t.Fatalf("b64 not decoded: %+v", got)
	}
	extra := gotBody["extra_body"].(map[string]any)
	imgs, ok := extra["image"].([]any)
	if !ok || len(imgs) != 1 || imgs[0] != "https://in/a.png" {
		t.Errorf("img2img reference not in extra_body.image: %v", extra)
	}
}

func TestVideoSubmitAndPoll(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/videos":
			body, _ := io.ReadAll(r.Body)
			var b map[string]any
			_ = json.Unmarshal(body, &b)
			if b["image"] != "https://in/a.png" {
				t.Errorf("single input image should be top-level image: %v", b)
			}
			io.WriteString(w, `{"video_id":"video_1","task_id":"task_1","status":"queued","progress":0}`)
		case r.Method == http.MethodGet && r.URL.Path == "/agnesapi":
			if r.URL.Query().Get("video_id") != "video_1" {
				t.Errorf("query missing video_id: %s", r.URL.RawQuery)
			}
			calls++
			if calls < 2 {
				io.WriteString(w, `{"video_id":"video_1","status":"in_progress","progress":40}`)
			} else {
				io.WriteString(w, `{"video_id":"video_1","status":"completed","progress":100,"seconds":"10.0","remixed_from_video_id":"https://cdn/out.mp4","error":null}`)
			}
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL)
		}
	}))
	defer srv.Close()

	m := Video("agnes-video-v2.0", "KEY", WithBaseURL(srv.URL), WithPollInterval(time.Millisecond))
	req := &llm.VideoRequest{Prompt: "a cat walks", InputImages: []core.Image{{URL: "https://in/a.png"}}}

	var statuses []llm.JobStatus
	var final *llm.VideoProgress
	for p, err := range m.GenerateVideo(context.Background(), req) {
		if err != nil {
			t.Fatalf("GenerateVideo: %v", err)
		}
		statuses = append(statuses, p.Status)
		final = p
	}
	// queued (submit) -> running -> succeeded
	want := []llm.JobStatus{llm.JobQueued, llm.JobRunning, llm.JobSucceeded}
	if strings.Join(toStrs(statuses), ",") != strings.Join(toStrs(want), ",") {
		t.Errorf("status sequence = %v, want %v", statuses, want)
	}
	if final.Video == nil || final.Video.URL != "https://cdn/out.mp4" || final.Video.DurationMs != 10000 {
		t.Fatalf("final video wrong: %+v", final.Video)
	}
	if final.JobID != "video_1" {
		t.Errorf("JobID = %q, want video_1", final.JobID)
	}
}

func TestVideoResume(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/agnesapi" {
			t.Errorf("resume should only query, got %s", r.URL.Path)
		}
		io.WriteString(w, `{"video_id":"video_9","status":"completed","progress":100,"seconds":"5.0","remixed_from_video_id":"https://cdn/r.mp4"}`)
	}))
	defer srv.Close()

	m := Video("agnes-video-v2.0", "KEY", WithBaseURL(srv.URL), WithPollInterval(time.Millisecond))
	var rm llm.ResumableVideoModel = m
	var final *llm.VideoProgress
	for p, err := range rm.ResumeVideo(context.Background(), "video_9") {
		if err != nil {
			t.Fatalf("ResumeVideo: %v", err)
		}
		final = p
	}
	if final == nil || final.Status != llm.JobSucceeded || final.Video.URL != "https://cdn/r.mp4" {
		t.Fatalf("resume final wrong: %+v", final)
	}
}

func toStrs(ss []llm.JobStatus) []string {
	out := make([]string, len(ss))
	for i, s := range ss {
		out[i] = string(s)
	}
	return out
}
