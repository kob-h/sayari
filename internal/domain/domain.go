// Package domain defines the core types and contracts shared across the
// document-processing pipeline: documents, tokens, the NLP/LLM data shapes, and
// the finite-state machines that govern processing.
//
// These types are deliberately free of infrastructure concerns (no database or
// Redis tags beyond what is strictly needed) so that the store, queue, NLP, and
// LLM packages can depend on them without depending on each other.
package domain

import (
	"fmt"
	"time"
)

// DocStatus is the processing state of a document. It is the source of truth for
// which pipeline stage a document is in and drives rerun/recovery decisions.
//
// Lifecycle: PENDING -> EXTRACTING -> CLASSIFYING -> COMPLETED.
// A document may move to FAILED from any active state on an unrecoverable error.
type DocStatus string

const (
	// DocPending means the document has been accepted but extraction has not started.
	DocPending DocStatus = "PENDING"
	// DocExtracting means an extraction worker has claimed the document and is producing tokens.
	DocExtracting DocStatus = "EXTRACTING"
	// DocClassifying means extraction finished and tokens are being classified.
	DocClassifying DocStatus = "CLASSIFYING"
	// DocCompleted means every token has been classified.
	DocCompleted DocStatus = "COMPLETED"
	// DocFailed means processing hit an unrecoverable error.
	DocFailed DocStatus = "FAILED"
)

// Valid reports whether s is a known document status.
func (s DocStatus) Valid() bool {
	switch s {
	case DocPending, DocExtracting, DocClassifying, DocCompleted, DocFailed:
		return true
	default:
		return false
	}
}

// TokenStatus is the processing state of an individual token.
//
// Lifecycle: PENDING -> CLASSIFIED.
type TokenStatus string

const (
	// TokenPending means the token has been extracted but not yet classified.
	TokenPending TokenStatus = "PENDING"
	// TokenClassified means the token has a classification persisted.
	TokenClassified TokenStatus = "CLASSIFIED"
)

// EntityType is the coarse type assigned by the NLP extraction stage. It mirrors
// common NLP labels (e.g. spaCy) and is intentionally distinct from the
// downstream business Category produced by classification.
type EntityType string

const (
	EntityPerson EntityType = "PERSON" // a person's name
	EntityOrg    EntityType = "ORG"    // an organization / company
	EntityGPE    EntityType = "GPE"    // geo-political entity (location)
	EntityDate   EntityType = "DATE"   // a date or time expression
	EntityMisc   EntityType = "MISC"   // anything else the NLP stage is unsure about
)

// Category is the business classification produced by the LLM stage.
type Category string

const (
	CategoryCompany Category = "COMPANY"
	CategoryPerson  Category = "PERSON"
	CategoryAddress Category = "ADDRESS"
	CategoryDate    Category = "DATE"
	CategoryUnknown Category = "UNKNOWN"
)

// Valid reports whether c is a known category.
func (c Category) Valid() bool {
	switch c {
	case CategoryCompany, CategoryPerson, CategoryAddress, CategoryDate, CategoryUnknown:
		return true
	default:
		return false
	}
}

// RerunMode selects how a process request treats an existing document.
type RerunMode string

const (
	// RerunPartial resumes an in-flight or failed document from where it left off.
	// It is also the default for a brand-new document.
	RerunPartial RerunMode = "partial"
	// RerunFull discards all prior tokens and reprocesses from scratch.
	RerunFull RerunMode = "full"
)

// Position records where an entity was found in the source document. Offsets are
// rune-based (not byte-based) so positions are stable for multi-byte text.
type Position struct {
	Page       int `json:"page"`
	Sentence   int `json:"sentence"`
	CharOffset int `json:"char_offset"`
}

// Entity is a raw extraction result returned by an Extractor. It carries no
// processing state; the store turns it into a Token.
type Entity struct {
	Text     string     `json:"text"`
	Type     EntityType `json:"type"`
	Position Position   `json:"position"`
}

// Document is the manifest for a single document's processing run. It is the
// authoritative record of state, progress, and stage timing.
type Document struct {
	ID          string    `json:"id"`
	Text        string    `json:"text"`
	ContentHash string    `json:"content_hash"`
	Status      DocStatus `json:"status"`
	// RunVersion is a fencing token bumped on every full rerun. Workers stamp it
	// onto their writes; the store rejects writes whose version is stale, so
	// in-flight workers from a superseded run cannot corrupt fresh data.
	RunVersion      int `json:"run_version"`
	TotalTokens     int `json:"total_tokens"`
	ClassifiedCount int `json:"classified_count"`

	ExtractionStartedAt       *time.Time `json:"extraction_started_at,omitempty"`
	ExtractionCompletedAt     *time.Time `json:"extraction_completed_at,omitempty"`
	ClassificationStartedAt   *time.Time `json:"classification_started_at,omitempty"`
	ClassificationCompletedAt *time.Time `json:"classification_completed_at,omitempty"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Token is an extracted entity plus its classification state. Each token is
// stored and processed individually so classification can scale per-token and
// resume mid-document.
type Token struct {
	ID            int64       `json:"id"`
	DocumentID    string      `json:"document_id"`
	RunVersion    int         `json:"run_version"`
	Text          string      `json:"text"`
	NLPEntityType EntityType  `json:"nlp_entity_type"`
	Position      Position    `json:"position"`
	Status        TokenStatus `json:"status"`

	Classification *Category `json:"classification,omitempty"`
	Confidence     *float64  `json:"confidence,omitempty"`
	Reasoning      *string   `json:"reasoning,omitempty"`

	CreatedAt    time.Time  `json:"created_at"`
	ClassifiedAt *time.Time `json:"classified_at,omitempty"`
}

// Classification is the result of the LLM stage for a single token.
type Classification struct {
	Category   Category `json:"category"`
	Confidence float64  `json:"confidence"`
	Reasoning  string   `json:"reasoning"`
}

// Durations returns the wall-clock time spent in each stage. A zero duration
// means the corresponding stage has not completed yet.
func (d Document) Durations() (extraction, classification time.Duration) {
	if d.ExtractionStartedAt != nil && d.ExtractionCompletedAt != nil {
		extraction = d.ExtractionCompletedAt.Sub(*d.ExtractionStartedAt)
	}
	if d.ClassificationStartedAt != nil && d.ClassificationCompletedAt != nil {
		classification = d.ClassificationCompletedAt.Sub(*d.ClassificationStartedAt)
	}
	return extraction, classification
}

// TokenFilter narrows a token query. Nil/empty fields are ignored, so the zero
// value matches every token for a document.
type TokenFilter struct {
	Classification *Category
	NLPEntityType  *EntityType
	Status         *TokenStatus
	Page           *int
	Limit          int
	Offset         int
}

// ErrStaleWrite is returned by the store when a worker attempts to write using a
// run version older than the document's current version. It signals the worker
// to drop the (superseded) result rather than retry.
var ErrStaleWrite = fmt.Errorf("stale write: run version superseded")

// ErrNotFound is returned when a requested document or token does not exist.
var ErrNotFound = fmt.Errorf("not found")
