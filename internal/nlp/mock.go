package nlp

import (
	"context"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/kob-h/docpipeline/internal/domain"
)

// MockExtractor is a deterministic, rule-based Extractor. It uses ordered
// regular expressions to find candidate entity spans (date-like, address-like,
// organization-like, and person-like text) and returns each as an untyped token
// with its sentence as context and its position. Per the pipeline's contract,
// extraction does NOT label entities — it only locates candidate tokens; the
// classifier decides what each one is. The patterns here serve only to find
// plausible spans, not to categorize them. Dependency-free and reproducible.
type MockExtractor struct {
	rules []rule
}

type rule struct {
	// nameLike marks the person/proper-noun pattern, the only one that needs the
	// title/headline filtering below. It does not label the emitted token.
	nameLike bool
	re       *regexp.Regexp
}

// Patterns are evaluated in priority order; earlier matches win over later
// overlapping ones (so "Acme Corp" is captured as one span, not split by the
// person pattern). The categories in the names are only describing what each
// pattern tends to match — the output token is untyped.
func NewMockExtractor() *MockExtractor {
	return &MockExtractor{rules: []rule{
		{re: regexp.MustCompile(
			`\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}(?:st|nd|rd|th)?(?:,\s*\d{4})?\b`)},
		{re: regexp.MustCompile(`\b\d{1,2}/\d{1,2}/\d{2,4}\b`)},
		{re: regexp.MustCompile(`\b(?:19|20)\d{2}\b`)},
		{re: regexp.MustCompile(
			`\b\d{1,5}\s+(?:[A-Z][a-zA-Z]+\.?\s+){1,3}(?:Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Lane|Ln|Drive|Dr|Way|Court|Ct|Place|Pl|Square|Sq)\b\.?`)},
		{re: regexp.MustCompile(
			`\b(?:[A-Z][A-Za-z&.\-]+\s+){0,4}(?:Inc|Incorporated|Corp|Corporation|Company|Co|Ltd|LLC|PLC|Group|Technologies|Systems|Solutions|Holdings|Industries|Bank|Partners|Capital|Ventures|University|Institute|Association|Foundation|Agency|Department|Ministry)\b\.?`)},
		{nameLike: true, re: regexp.MustCompile(
			`\b(?:(?:Mr|Mrs|Ms|Dr|Prof|President|CEO|CFO|CTO|Senator|Governor|Mayor|Chairman)\.?\s+)?[A-Z][a-z]+(?:\s+[A-Z]\.)?(?:\s+[A-Z][a-z]+){1,2}\b`)},
	}}
}

// Extract implements Extractor. It scans each sentence with each rule, drops
// overlapping matches by rule priority, and returns entities sorted by position.
func (m *MockExtractor) Extract(_ context.Context, doc domain.Document) ([]domain.Entity, error) {
	var entities []domain.Entity
	for _, s := range splitSentences(doc.Text) {
		entities = append(entities, m.extractSentence(s)...)
	}
	sort.SliceStable(entities, func(i, j int) bool {
		return entities[i].Position.CharOffset < entities[j].Position.CharOffset
	})
	return entities, nil
}

func (m *MockExtractor) extractSentence(s sentence) []domain.Entity {
	type span struct {
		start, end int // byte offsets within the sentence
		text       string
	}
	var spans []span
	occupied := func(start, end int) bool {
		for _, sp := range spans {
			if start < sp.end && end > sp.start {
				return true
			}
		}
		return false
	}
	for _, r := range m.rules {
		for _, loc := range r.re.FindAllStringIndex(s.text, -1) {
			if occupied(loc[0], loc[1]) {
				continue
			}
			text := strings.TrimRight(strings.TrimSpace(s.text[loc[0]:loc[1]]), ".")
			if text == "" {
				continue
			}
			// Drop name-like matches that are really titles or headline phrases
			// (e.g. "Chief Executive Officer", "Announces New Leadership") so they
			// aren't emitted as candidate tokens. A real NLP model would avoid these;
			// the stub guards against the most common false positives.
			if r.nameLike && (isAllRoleWords(text) || containsNonNameWord(text)) {
				continue
			}
			spans = append(spans, span{loc[0], loc[1], text})
		}
	}
	sort.Slice(spans, func(i, j int) bool { return spans[i].start < spans[j].start })

	context := strings.TrimSpace(s.text)
	out := make([]domain.Entity, 0, len(spans))
	for _, sp := range spans {
		runeOff := s.runeStart + utf8.RuneCountInString(s.text[:sp.start])
		out = append(out, domain.Entity{
			Text:    sp.text,
			Context: context, // the entity's sentence, passed to classification
			Position: domain.Position{
				Page:       pageForOffset(s.docPrefix),
				Sentence:   s.index,
				CharOffset: runeOff,
			},
		})
	}
	return out
}

// sentence is one sentence plus the offsets needed to locate it in the document.
type sentence struct {
	text      string
	index     int    // 0-based sentence number within the document
	runeStart int    // rune offset of the sentence start within the document
	docPrefix string // document text up to the sentence start (for page counting)
}

// sentenceBoundary splits on sentence-ending punctuation followed by whitespace,
// or on line breaks. Splitting on newlines keeps headings and list items as their
// own segments, so an entity match cannot span a paragraph break.
var sentenceBoundary = regexp.MustCompile(`[.!?]+\s+|\n+`)

// splitSentences breaks text into sentences, preserving each sentence's rune
// offset within the original document. Segment boundaries are the start of the
// text plus the end of each sentence-terminator match.
func splitSentences(text string) []sentence {
	starts := []int{0}
	for _, l := range sentenceBoundary.FindAllStringIndex(text, -1) {
		starts = append(starts, l[1])
	}

	var out []sentence
	idx := 0
	for i, start := range starts {
		end := len(text)
		if i+1 < len(starts) {
			end = starts[i+1]
		}
		if start >= end || strings.TrimSpace(text[start:end]) == "" {
			continue
		}
		out = append(out, sentence{
			text:      text[start:end],
			index:     idx,
			runeStart: utf8.RuneCountInString(text[:start]),
			docPrefix: text[:start],
		})
		idx++
	}
	return out
}

// pageForOffset treats each form-feed in the preceding text as a page break.
func pageForOffset(prefix string) int {
	return 1 + strings.Count(prefix, "\f")
}

// roleWords are tokens that appear in job titles. A capitalized sequence made
// only of these is a title, not a person's name.
var roleWords = map[string]bool{
	"Chief": true, "Executive": true, "Officer": true, "Financial": true,
	"Technology": true, "Operating": true, "President": true, "Vice": true,
	"Senior": true, "Managing": true, "Director": true, "Chairman": true,
	"Chairwoman": true, "Board": true, "Member": true, "Head": true,
}

// isAllRoleWords reports whether every space-separated word in s is a role word.
func isAllRoleWords(s string) bool {
	fields := strings.Fields(s)
	if len(fields) == 0 {
		return false
	}
	for _, w := range fields {
		if !roleWords[w] {
			return false
		}
	}
	return true
}

// nonNameWords are capitalized function words and common headline verbs that
// appear at sentence/heading starts but are never part of a personal name.
var nonNameWords = map[string]bool{
	"The": true, "This": true, "That": true, "On": true, "In": true, "At": true,
	"Of": true, "And": true, "For": true, "With": true, "During": true, "After": true,
	"Before": true, "New": true, "Leadership": true, "Announces": true, "Announced": true,
	"Announce": true, "Reports": true, "Report": true, "Names": true, "Appoints": true,
	"Appointed": true, "Joins": true, "Welcomes": true, "Summit": true, "Annual": true,
	"Regional": true, "He": true, "She": true, "They": true, "We": true, "It": true,
	"More": true, "Visit": true,
}

// containsNonNameWord reports whether any word in s is a known non-name word.
func containsNonNameWord(s string) bool {
	for _, w := range strings.Fields(s) {
		if nonNameWords[w] {
			return true
		}
	}
	return false
}
