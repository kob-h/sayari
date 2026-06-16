// Package api exposes the pipeline over HTTP: submit documents, check status and
// progress, and query classified tokens. Handlers are deliberately thin — they
// parse the request, delegate to the application service, and render a DTO. All
// business logic lives in internal/service; all persistence in internal/store.
package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/kob-h/docpipeline/internal/domain"
	"github.com/kob-h/docpipeline/internal/service"
)

// Server holds the API dependencies.
type Server struct {
	svc *service.DocumentService
	log *slog.Logger
}

// NewServer constructs an API Server over the document service.
func NewServer(svc *service.DocumentService, log *slog.Logger) *Server {
	return &Server{svc: svc, log: log}
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

	res, err := s.svc.Submit(r.Context(), req.DocumentID, req.Text, req.rerunMode())
	if err != nil {
		s.log.Error("submit failed", "doc", req.DocumentID, "err", err)
		writeError(w, http.StatusInternalServerError, "could not accept document")
		return
	}
	writeJSON(w, http.StatusAccepted, newProcessResponse(res))
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	doc, err := s.svc.Status(r.Context(), r.PathValue("id"))
	if errors.Is(err, domain.ErrNotFound) {
		writeError(w, http.StatusNotFound, "document not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "lookup failed")
		return
	}
	writeJSON(w, http.StatusOK, newStatusResponse(doc))
}

func (s *Server) handleTokens(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	tokens, err := s.svc.Tokens(r.Context(), id, tokenFilterFromQuery(r))
	if errors.Is(err, domain.ErrNotFound) {
		writeError(w, http.StatusNotFound, "document not found")
		return
	}
	if err != nil {
		s.log.Error("list tokens failed", "doc", id, "err", err)
		writeError(w, http.StatusInternalServerError, "query failed")
		return
	}
	writeJSON(w, http.StatusOK, tokensResponse{DocumentID: id, Count: len(tokens), Tokens: tokens})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if err := s.svc.Health(r.Context()); err != nil {
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
