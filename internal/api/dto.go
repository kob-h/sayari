package api

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/kob-h/docpipeline/internal/domain"
	"github.com/kob-h/docpipeline/internal/service"
)

// This file holds the HTTP request/response shapes (DTOs) and the helpers that
// map between them and the domain/service types. Keeping them out of server.go
// keeps the handlers focused on request handling.

// --- POST /process ----------------------------------------------------------

type processRequest struct {
	DocumentID string `json:"document_id"`
	Text       string `json:"text"`
	Mode       string `json:"mode"` // "partial" (default) or "full"
}

// rerunMode normalises the request's mode field to a domain value.
func (r processRequest) rerunMode() domain.RerunMode {
	if r.Mode == string(domain.RerunFull) {
		return domain.RerunFull
	}
	return domain.RerunPartial
}

type processResponse struct {
	DocumentID string `json:"document_id"`
	Status     string `json:"status"`
	RunVersion int    `json:"run_version"`
	Accepted   bool   `json:"accepted"`
	Message    string `json:"message"`
}

func newProcessResponse(res service.SubmitResult) processResponse {
	return processResponse{
		DocumentID: res.Document.ID,
		Status:     string(res.Document.Status),
		RunVersion: res.Document.RunVersion,
		Accepted:   res.Accepted,
		Message:    submitMessage(res),
	}
}

// submitMessage renders the user-facing message for a submit outcome
// (presentation logic, derived from the business result).
func submitMessage(res service.SubmitResult) string {
	switch {
	case !res.Accepted:
		return "already completed; resubmit with mode=full to reprocess"
	case res.FullRerun:
		return "accepted for full reprocessing"
	default:
		return "accepted for processing"
	}
}

// --- GET /documents/{id}/status ---------------------------------------------

type progress struct {
	Classified int     `json:"classified"`
	Total      int     `json:"total"`
	Percent    float64 `json:"percent"`
}

type durations struct {
	ExtractionSeconds     float64 `json:"extraction_seconds"`
	ClassificationSeconds float64 `json:"classification_seconds"`
}

type statusResponse struct {
	DocumentID string         `json:"document_id"`
	Status     string         `json:"status"`
	RunVersion int            `json:"run_version"`
	Progress   progress       `json:"progress"`
	Durations  durations      `json:"durations"`
	Timestamps map[string]any `json:"timestamps"`
}

func newStatusResponse(doc domain.Document) statusResponse {
	ext, class := doc.Durations()
	pct := 0.0
	if doc.TotalTokens > 0 {
		pct = float64(doc.ClassifiedCount) / float64(doc.TotalTokens) * 100
	}
	return statusResponse{
		DocumentID: doc.ID,
		Status:     string(doc.Status),
		RunVersion: doc.RunVersion,
		Progress:   progress{Classified: doc.ClassifiedCount, Total: doc.TotalTokens, Percent: round2(pct)},
		Durations: durations{
			ExtractionSeconds:     round2(ext.Seconds()),
			ClassificationSeconds: round2(class.Seconds()),
		},
		Timestamps: map[string]any{
			"extraction_started_at":       doc.ExtractionStartedAt,
			"extraction_completed_at":     doc.ExtractionCompletedAt,
			"classification_started_at":   doc.ClassificationStartedAt,
			"classification_completed_at": doc.ClassificationCompletedAt,
			"created_at":                  doc.CreatedAt,
			"updated_at":                  doc.UpdatedAt,
		},
	}
}

// --- GET /documents/{id}/tokens ---------------------------------------------

type tokensResponse struct {
	DocumentID string         `json:"document_id"`
	Count      int            `json:"count"`
	Tokens     []domain.Token `json:"tokens"`
}

// tokenFilterFromQuery builds a domain.TokenFilter from the request query string.
func tokenFilterFromQuery(r *http.Request) domain.TokenFilter {
	q := r.URL.Query()
	f := domain.TokenFilter{Limit: queryInt(r, "limit", 0), Offset: queryInt(r, "offset", 0)}
	if v := q.Get("classification"); v != "" {
		c := domain.Category(strings.ToUpper(v))
		f.Classification = &c
	}
	if v := q.Get("status"); v != "" {
		st := domain.TokenStatus(strings.ToUpper(v))
		f.Status = &st
	}
	if v := q.Get("page"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			f.Page = &p
		}
	}
	return f
}
