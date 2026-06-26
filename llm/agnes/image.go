package agnes

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"net/http"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
)

const defaultImageSize = "1024x768"

// ImageModel is an Agnes image-generation model (e.g. "agnes-image-2.1-flash").
type ImageModel struct {
	model string
	cfg   config
}

// Image constructs an Agnes image model. Use WithBaseURL to target a non-default
// deployment.
func Image(model, apiKey string, opts ...Option) *ImageModel {
	return &ImageModel{model: model, cfg: newConfig(apiKey, opts...)}
}

func (m *ImageModel) Name() string { return m.model }

// GenerateImage implements llm.ImageModel. The Agnes image endpoint is
// synchronous, so the returned stream yields exactly one response.
func (m *ImageModel) GenerateImage(ctx context.Context, req *llm.ImageRequest) iter.Seq2[*llm.ImageResponse, error] {
	return func(yield func(*llm.ImageResponse, error) bool) {
		body, err := json.Marshal(m.buildBody(req))
		if err != nil {
			yield(nil, err)
			return
		}
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, m.cfg.baseURL+"/v1/images/generations", bytes.NewReader(body))
		if err != nil {
			yield(nil, err)
			return
		}
		httpReq.Header.Set("content-type", "application/json")
		httpReq.Header.Set("authorization", "Bearer "+m.cfg.apiKey)

		resp, err := m.cfg.http.Do(httpReq)
		if err != nil {
			yield(nil, err)
			return
		}
		defer resp.Body.Close()

		data, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK {
			yield(nil, fmt.Errorf("agnes image: status %d: %s", resp.StatusCode, data))
			return
		}
		out, err := parseImageResponse(data)
		if err != nil {
			yield(nil, err)
			return
		}
		yield(out, nil)
	}
}

// imageReqBody mirrors the Agnes image request. Per the docs: model/prompt/size
// are top-level; response_format and image-to-image inputs go inside a nested
// extra_body object (putting response_format at the top level returns 400). A
// bare text-to-image base64 request uses the top-level return_base64 flag.
type imageReqBody struct {
	Model        string         `json:"model"`
	Prompt       string         `json:"prompt"`
	Size         string         `json:"size"`
	ReturnBase64 bool           `json:"return_base64,omitempty"`
	ExtraBody    map[string]any `json:"extra_body,omitempty"`
}

func (m *ImageModel) buildBody(req *llm.ImageRequest) imageReqBody {
	b := imageReqBody{
		Model:  pick(req.Options.Model, m.model),
		Prompt: req.Prompt,
		Size:   pick(req.Options.Size, defaultImageSize),
	}
	// We always request URL output (a link flows naturally as core.Image{URL}).
	// extra_body is also where image-to-image references live, so we route both
	// through it; a plain text-to-image call still works with just response_format.
	extra := map[string]any{"response_format": "url"}
	if refs := imageRefs(req.InputImages); len(refs) > 0 {
		extra["image"] = refs // image-to-image (URLs or Data URIs)
	}
	b.ExtraBody = extra
	return b
}

type imageRespBody struct {
	Created int64 `json:"created"`
	Data    []struct {
		URL     string `json:"url"`
		B64JSON string `json:"b64_json"`
	} `json:"data"`
}

func parseImageResponse(data []byte) (*llm.ImageResponse, error) {
	var wr imageRespBody
	if err := json.Unmarshal(data, &wr); err != nil {
		return nil, fmt.Errorf("agnes image: decode response: %w", err)
	}
	if len(wr.Data) == 0 {
		return nil, fmt.Errorf("agnes image: no data in response")
	}
	var imgs []core.Image
	for _, d := range wr.Data {
		switch {
		case d.URL != "":
			imgs = append(imgs, core.Image{URL: d.URL})
		case d.B64JSON != "":
			raw, err := base64.StdEncoding.DecodeString(d.B64JSON)
			if err != nil {
				return nil, fmt.Errorf("agnes image: decode b64_json: %w", err)
			}
			imgs = append(imgs, core.Image{MIME: "image/png", Data: raw})
		}
	}
	return &llm.ImageResponse{Images: imgs}, nil
}

var _ llm.ImageModel = (*ImageModel)(nil)
