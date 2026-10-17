// Package feel is a native interpreter for FEEL (Friendly Enough
// Expression Language, the DMN/BPMN expression language Camunda 8 uses),
// covering the subset process models use in practice: literals, arithmetic
// on exact decimals, comparisons, boolean logic with FEEL's three-valued
// null semantics, if/then/else, for/some/every, lists, contexts, ranges,
// filters, paths, unary tests and the common built-in functions.
//
// Unlike the FEEL-to-CEL rewriting in internal/common it parses FEEL
// itself, so FEEL semantics hold: a missing variable is null rather than an
// error, 0.1 + 0.2 = 0.3, and FEEL-only constructs work.
package feel

import (
	"fmt"
	"strings"
	"unicode"
)

type tokenKind int

const (
	tokEOF tokenKind = iota
	tokNumber
	tokString
	tokName
	tokTemporal // @"2024-01-01"
	tokPunct
)

type token struct {
	kind tokenKind
	text string
	pos  int
}

func (t token) is(kind tokenKind, text string) bool { return t.kind == kind && t.text == text }

// punctuation, longest first so ".." wins over "." and "**" over "*".
var punctuation = []string{"..", "**", "!=", "<=", ">=", "(", ")", "[", "]", "{", "}", ",", ":", ".", "+", "-", "*", "/", "=", "<", ">"}

func lex(src string) ([]token, error) {
	var toks []token
	r := []rune(src)
	i := 0
	for i < len(r) {
		c := r[i]
		switch {
		case unicode.IsSpace(c):
			i++
		case c == '/' && i+1 < len(r) && r[i+1] == '/':
			for i < len(r) && r[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < len(r) && r[i+1] == '*':
			end := strings.Index(string(r[i+2:]), "*/")
			if end < 0 {
				return nil, fmt.Errorf("unterminated comment at %d", i)
			}
			i += 2 + len([]rune(string(r[i+2:])[:end])) + 2
		case unicode.IsDigit(c) || (c == '.' && i+1 < len(r) && unicode.IsDigit(r[i+1]) && !(i > 0 && r[i-1] == '.')):
			start := i
			for i < len(r) && unicode.IsDigit(r[i]) {
				i++
			}
			// A fraction, but not the ".." of a range like 1..10.
			if i+1 < len(r) && r[i] == '.' && unicode.IsDigit(r[i+1]) {
				i++
				for i < len(r) && unicode.IsDigit(r[i]) {
					i++
				}
			}
			toks = append(toks, token{tokNumber, string(r[start:i]), start})
		case c == '"' || c == '\'':
			s, n, err := lexString(r[i:], c)
			if err != nil {
				return nil, fmt.Errorf("%v at %d", err, i)
			}
			toks = append(toks, token{tokString, s, i})
			i += n
		case c == '@' && i+1 < len(r) && r[i+1] == '"':
			s, n, err := lexString(r[i+1:], '"')
			if err != nil {
				return nil, fmt.Errorf("%v at %d", err, i)
			}
			toks = append(toks, token{tokTemporal, s, i})
			i += 1 + n
		case unicode.IsLetter(c) || c == '_' || c == '?' || c == '$':
			start := i
			i++
			for i < len(r) && (unicode.IsLetter(r[i]) || unicode.IsDigit(r[i]) || r[i] == '_' || r[i] == '$') {
				i++
			}
			toks = append(toks, token{tokName, string(r[start:i]), start})
		default:
			matched := false
			for _, p := range punctuation {
				if strings.HasPrefix(string(r[i:]), p) {
					toks = append(toks, token{tokPunct, p, i})
					i += len([]rune(p))
					matched = true
					break
				}
			}
			if !matched {
				return nil, fmt.Errorf("unexpected character %q at %d", c, i)
			}
		}
	}
	return append(toks, token{tokEOF, "", len(r)}), nil
}

// lexString reads a quoted string starting at r[0] and returns its value
// and the number of runes consumed.
func lexString(r []rune, quote rune) (string, int, error) {
	var b strings.Builder
	for i := 1; i < len(r); i++ {
		c := r[i]
		if c == quote {
			return b.String(), i + 1, nil
		}
		if c == '\\' && i+1 < len(r) {
			i++
			switch r[i] {
			case 'n':
				b.WriteRune('\n')
			case 't':
				b.WriteRune('\t')
			case 'r':
				b.WriteRune('\r')
			case 'u':
				if i+4 < len(r) {
					var code rune
					if _, err := fmt.Sscanf(string(r[i+1:i+5]), "%04x", &code); err == nil {
						b.WriteRune(code)
						i += 4
						continue
					}
				}
				b.WriteRune('u')
			default:
				b.WriteRune(r[i])
			}
			continue
		}
		b.WriteRune(c)
	}
	return "", 0, fmt.Errorf("unterminated string")
}
