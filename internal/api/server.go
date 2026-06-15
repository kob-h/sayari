// Package api exposes the pipeline over HTTP: submit documents, check status and
// progress, and query classified tokens. The API server only writes the initial
// document record and publishes the first extract job — all heavy work happens
// asynchronously in the worker stages, which is what lets the API scale
// independently of processing throughput.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/kob-h/docpipeline/internal/domain"
	"github.com/kob-h/docpipeline/internal/pipeline"
	"github.com/kob-h/docpipeline/internal/queue"
	"github.com/kob-h/docpipeline/internal/store"
)

// Server holds the API dependencies.
type Server struct {
	store  *store.Store
	broker queue.Broker
	log    *slog.Logger
}

// NewServer constructs an API Server.
func NewServer(s *store.Store, b queue.Broker, log *slog.Logger) *Server {
	return &Server{store: s, broker: b, log: log}
}

// Handler returns the HTTP handler with all routes registered (Go 1.22 method
// patterns).
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /process", s.handleProcess)
	mux.HandleFunc("GET /documents/{id}/status", s.handleStatus)
	mux.HandleFunc("GET /documents/{id}/tokens", s.handleTokens)
	mux.HandleFunc("GET /healthz", s.handleHealth)
	return logging(s.log, mux)
}

type processRequest struct {
	DocumentID string `json:"document_id"`
	Text       string `json:"text"`
	Mode       string `json:"mode"` // "partial" (default) or "full"
}

type processResponse struct {
	DocumentID string `json:"document_id"`
	Status     string `json:"status"`
	RunVersion int    `json:"run_version"`
	Accepted   bool   `json:"accepted"`
	Message    string `json:"message"`
}

func (s *Server) handleProcess(w http.ResponseWriter, r *http.Request) {
	var req processRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if strings.TrimSpace(req.DocumentID) == "" || strings.TrimSpace(req.Text) == "" {
		writeError(w, http.StatusBadRequest, "document_id and text are required")
		return
	}
	mode := domain.RerunPartial
	if req.Mode == string(domain.RerunFull) {
		mode = domain.RerunFull
	}

	res, err := s.store.AcceptDocument(r.Context(), req.DocumentID, req.Text, mode)
	if err != nil {
		s.log.Error("accept document failed", "doc", req.DocumentID, "err", err)
		writeError(w, http.StatusInternalServerError, "could not accept document")
		return
	}

	msg := "already completed; resubmit with mode=full to reprocess"
	if res.Enqueue {
		if err := pipeline.PublishExtract(r.Context(), s.broker, pipeline.ExtractJob{
			DocumentID: res.Document.ID, RunVersion: res.Document.RunVersion,
		}); err != nil {
			// State is persisted; the reconciler will re-enqueue. Report 202.
			s.log.Warn("publish extract failed; reconciler will recover", "doc", req.DocumentID, "err", err)
		}
		msg = "accepted for processing"
		if res.WasFullRerun {
			msg = "accepted for full reprocessing"
		}
	}

	writeJSON(w, http.StatusAccepted, processResponse{
		DocumentID: res.Document.ID,
		Status:     string(res.Document.Status),
		RunVersion: res.Document.RunVersion,
		Accepted:   res.Enqueue,
		Message:    msg,
	})
}

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

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	doc, err := s.store.GetDocument(r.Context(), id)
	if errors.Is(err, domain.ErrNotFound) {
		writeError(w, http.StatusNotFound, "document not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "lookup failed")
		return
	}

	ext, class := doc.Durations()
	pct := 0.0
	if doc.TotalTokens > 0 {
		pct = float64(doc.ClassifiedCount) / float64(doc.TotalTokens) * 100
	}

	writeJSON(w, http.StatusOK, statusResponse{
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
	})
}

type tokensResponse struct {
	DocumentID string         `json:"document_id"`
	Count      int            `json:"count"`
	Tokens     []domain.Token `json:"tokens"`
}

func (s *Server) handleTokens(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := s.store.GetDocument(r.Context(), id); errors.Is(err, domain.ErrNotFound) {
		writeError(w, http.StatusNotFound, "document not found")
		return
	}

	f := domain.TokenFilter{Limit: queryInt(r, "limit", 0), Offset: queryInt(r, "offset", 0)}
	if v := r.URL.Query().Get("classification"); v != "" {
		c := domain.Category(strings.ToUpper(v))
		f.Classification = &c
	}
	if v := r.URL.Query().Get("type"); v != "" {
		t := domain.EntityType(strings.ToUpper(v))
		f.NLPEntityType = &t
	}
	if v := r.URL.Query().Get("status"); v != "" {
		st := domain.TokenStatus(strings.ToUpper(v))
		f.Status = &st
	}
	if v := r.URL.Query().Get("page"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			f.Page = &p
		}
	}

	tokens, err := s.store.ListTokens(r.Context(), id, f)
	if err != nil {
		s.log.Error("list tokens failed", "doc", id, "err", err)
		writeError(w, http.StatusInternalServerError, "query failed")
		return
	}
	writeJSON(w, http.StatusOK, tokensResponse{DocumentID: id, Count: len(tokens), Tokens: tokens})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.store.Ping(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, "postgres unavailable")
		return
	}
	if err := s.broker.Ping(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, "broker unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
