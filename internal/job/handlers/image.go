package handlers

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/disintegration/imaging"
)

// ImageResizePayload carries a client-supplied base64-encoded image rather
// than a URL to fetch or an S3 bucket to upload to — that keeps this handler
// free of any paid external service while still doing real image processing.
type ImageResizePayload struct {
	ImageBase64 string `json:"image_base64"`
	Width       int    `json:"width"`
	Height      int    `json:"height"`
}

type ImageResizeResult struct {
	OriginalSizeBytes int    `json:"original_size_bytes"`
	ResizedSizeBytes  int    `json:"resized_size_bytes"`
	Width             int    `json:"width"`
	Height            int    `json:"height"`
	ImageBase64       string `json:"image_base64"`
}

// ImageResizeHandler performs real, local image decoding/resizing/re-encoding
// via disintegration/imaging (pure Go, no cgo, no external service).
type ImageResizeHandler struct{}

func (h *ImageResizeHandler) Execute(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
	var p ImageResizePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("invalid image_resize payload: %w", err)
	}
	if p.Width <= 0 || p.Height <= 0 {
		return nil, fmt.Errorf("width and height must be positive")
	}
	if p.Width > 4096 || p.Height > 4096 {
		return nil, fmt.Errorf("width and height must not exceed 4096")
	}

	raw, err := base64.StdEncoding.DecodeString(p.ImageBase64)
	if err != nil {
		return nil, fmt.Errorf("invalid base64 image data: %w", err)
	}

	img, err := imaging.Decode(bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("failed to decode image: %w", err)
	}

	resized := imaging.Fit(img, p.Width, p.Height, imaging.Lanczos)

	var buf bytes.Buffer
	if err := imaging.Encode(&buf, resized, imaging.PNG); err != nil {
		return nil, fmt.Errorf("failed to encode resized image: %w", err)
	}

	out := ImageResizeResult{
		OriginalSizeBytes: len(raw),
		ResizedSizeBytes:  buf.Len(),
		Width:             resized.Bounds().Dx(),
		Height:            resized.Bounds().Dy(),
		ImageBase64:       base64.StdEncoding.EncodeToString(buf.Bytes()),
	}
	return json.Marshal(out)
}
