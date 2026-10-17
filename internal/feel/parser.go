package feel

import (
	"fmt"
	"math/big"
	"strings"
)

// node is a parsed FEEL expression.
type node interface{}

type (
	litNode  struct{ v value }
	nameNode struct{ name string }
	negNode  struct{ x node }
	binNode  struct {
		op   string
		l, r node
	}
	betweenNode struct{ x, lo, hi node }
	inNode      struct {
		x     node
		tests []node
	}
	ifNode   struct{ cond, then, els node }
	iterator struct {
		name string
		in   node
	}
	forNode struct {
		iters []iterator
		body  node
	}
	quantNode struct {
		every bool
		iters []iterator
		cond  node
	}
	listNode    struct{ items []node }
	contextNode struct {
		keys []string
		vals []node
	}
	rangeNode struct {
		lo, hi             node
		loClosed, hiClosed bool
	}
	pathNode struct {
		x    node
		name string
	}
	filterNode struct{ x, cond node }
	callNode   struct {
		fn    string
		args  []node
		named map[string]node
	}
	// cmpTestNode is a unary test like "< 10" (compare the tested value).
	cmpTestNode struct {
		op string
		x  node
	}
	anyTestNode struct{}
	notTestNode struct{ tests []node }
)

type parser struct {
	toks []token
	pos  int
}

func (p *parser) peek() token { return p.toks[p.pos] }
func (p *parser) peekAt(n int) token {
	if p.pos+n < len(p.toks) {
		return p.toks[p.pos+n]
	}
	return p.toks[len(p.toks)-1]
}
func (p *parser) next() token { t := p.toks[p.pos]; p.pos++; return t }

func (p *parser) accept(kind tokenKind, text string) bool {
	if p.peek().is(kind, text) {
		p.pos++
		return true
	}
	return false
}

func (p *parser) keyword(word string) bool { return p.accept(tokName, word) }

func (p *parser) expect(kind tokenKind, text string) error {
	if !p.accept(kind, text) {
		t := p.peek()
		if t.kind == tokEOF {
			return fmt.Errorf("expected %q, found end of expression", text)
		}
		return fmt.Errorf("expected %q at %d, found %q", text, t.pos, t.text)
	}
	return nil
}

func parseExpression(src string) (node, error) {
	toks, err := lex(src)
	if err != nil {
		return nil, err
	}
	p := &parser{toks: toks}
	n, err := p.expr()
	if err != nil {
		return nil, err
	}
	if t := p.peek(); t.kind != tokEOF {
		return nil, fmt.Errorf("unexpected %q at %d", t.text, t.pos)
	}
	return n, nil
}

func (p *parser) expr() (node, error) {
	switch t := p.peek(); {
	case t.is(tokName, "if"):
		p.next()
		cond, err := p.expr()
		if err != nil {
			return nil, err
		}
		if err := p.expect(tokName, "then"); err != nil {
			return nil, err
		}
		then, err := p.expr()
		if err != nil {
			return nil, err
		}
		if err := p.expect(tokName, "else"); err != nil {
			return nil, err
		}
		els, err := p.expr()
		if err != nil {
			return nil, err
		}
		return &ifNode{cond, then, els}, nil
	case t.is(tokName, "for"):
		p.next()
		iters, err := p.iterators()
		if err != nil {
			return nil, err
		}
		if err := p.expect(tokName, "return"); err != nil {
			return nil, err
		}
		body, err := p.expr()
		if err != nil {
			return nil, err
		}
		return &forNode{iters, body}, nil
	case t.is(tokName, "some") || t.is(tokName, "every"):
		p.next()
		iters, err := p.iterators()
		if err != nil {
			return nil, err
		}
		if err := p.expect(tokName, "satisfies"); err != nil {
			return nil, err
		}
		cond, err := p.expr()
		if err != nil {
			return nil, err
		}
		return &quantNode{t.text == "every", iters, cond}, nil
	}
	return p.disjunction()
}

func (p *parser) iterators() ([]iterator, error) {
	var iters []iterator
	for {
		name := p.next()
		if name.kind != tokName {
			return nil, fmt.Errorf("expected a variable name at %d", name.pos)
		}
		if err := p.expect(tokName, "in"); err != nil {
			return nil, err
		}
		in, err := p.disjunction()
		if err != nil {
			return nil, err
		}
		// "for i in 1..3" — a bare range without brackets.
		if p.accept(tokPunct, "..") {
			hi, err := p.disjunction()
			if err != nil {
				return nil, err
			}
			in = &rangeNode{lo: in, hi: hi, loClosed: true, hiClosed: true}
		}
		iters = append(iters, iterator{name.text, in})
		if !p.accept(tokPunct, ",") {
			return iters, nil
		}
	}
}

func (p *parser) disjunction() (node, error) {
	l, err := p.conjunction()
	if err != nil {
		return nil, err
	}
	for p.keyword("or") {
		r, err := p.conjunction()
		if err != nil {
			return nil, err
		}
		l = &binNode{"or", l, r}
	}
	return l, nil
}

func (p *parser) conjunction() (node, error) {
	l, err := p.comparison()
	if err != nil {
		return nil, err
	}
	for p.keyword("and") {
		r, err := p.comparison()
		if err != nil {
			return nil, err
		}
		l = &binNode{"and", l, r}
	}
	return l, nil
}

var comparisonOps = map[string]bool{"=": true, "!=": true, "<": true, "<=": true, ">": true, ">=": true}

func (p *parser) comparison() (node, error) {
	l, err := p.additive()
	if err != nil {
		return nil, err
	}
	t := p.peek()
	switch {
	case t.kind == tokPunct && comparisonOps[t.text]:
		p.next()
		r, err := p.additive()
		if err != nil {
			return nil, err
		}
		return &binNode{t.text, l, r}, nil
	case t.is(tokName, "between"):
		p.next()
		lo, err := p.additive()
		if err != nil {
			return nil, err
		}
		if err := p.expect(tokName, "and"); err != nil {
			return nil, err
		}
		hi, err := p.additive()
		if err != nil {
			return nil, err
		}
		return &betweenNode{l, lo, hi}, nil
	case t.is(tokName, "in"):
		p.next()
		tests, err := p.positiveUnaryTests()
		if err != nil {
			return nil, err
		}
		return &inNode{l, tests}, nil
	}
	return l, nil
}

// positiveUnaryTests parses the right side of "in": one test, or a
// parenthesized comma-separated list of tests.
func (p *parser) positiveUnaryTests() ([]node, error) {
	if p.peek().is(tokPunct, "(") && !p.looksLikeRange() {
		p.next()
		var tests []node
		for {
			t, err := p.unaryTest()
			if err != nil {
				return nil, err
			}
			tests = append(tests, t)
			if p.accept(tokPunct, ")") {
				return tests, nil
			}
			if err := p.expect(tokPunct, ","); err != nil {
				return nil, err
			}
		}
	}
	t, err := p.unaryTest()
	if err != nil {
		return nil, err
	}
	return []node{t}, nil
}

// looksLikeRange reports whether the bracket at the cursor opens a range
// ("(1..5]") rather than a parenthesized expression or test list.
func (p *parser) looksLikeRange() bool {
	depth := 0
	for i := p.pos; i < len(p.toks); i++ {
		t := p.toks[i]
		if t.kind == tokPunct {
			switch t.text {
			case "(", "[", "{":
				depth++
			case ")", "]", "}":
				depth--
				if depth <= 0 {
					return false
				}
			case "..":
				if depth == 1 {
					return true
				}
			case ",":
				if depth == 1 {
					return false
				}
			}
		}
		if t.kind == tokEOF {
			return false
		}
	}
	return false
}

// unaryTest parses one unary test: "-", "< expr", or an expression
// (a range, a list, a value to compare for equality, or a boolean using ?).
func (p *parser) unaryTest() (node, error) {
	t := p.peek()
	if t.is(tokPunct, "-") && (p.peekAt(1).kind == tokEOF || p.peekAt(1).is(tokPunct, ",") || p.peekAt(1).is(tokPunct, ")")) {
		p.next()
		return &anyTestNode{}, nil
	}
	if t.kind == tokPunct && comparisonOps[t.text] && t.text != "=" {
		p.next()
		x, err := p.additive()
		if err != nil {
			return nil, err
		}
		return &cmpTestNode{t.text, x}, nil
	}
	if t.is(tokPunct, "=") {
		p.next()
		x, err := p.additive()
		if err != nil {
			return nil, err
		}
		return &cmpTestNode{"=", x}, nil
	}
	return p.expr()
}

func (p *parser) additive() (node, error) {
	l, err := p.multiplicative()
	if err != nil {
		return nil, err
	}
	for {
		t := p.peek()
		if !t.is(tokPunct, "+") && !t.is(tokPunct, "-") {
			return l, nil
		}
		p.next()
		r, err := p.multiplicative()
		if err != nil {
			return nil, err
		}
		l = &binNode{t.text, l, r}
	}
}

func (p *parser) multiplicative() (node, error) {
	l, err := p.exponent()
	if err != nil {
		return nil, err
	}
	for {
		t := p.peek()
		if !t.is(tokPunct, "*") && !t.is(tokPunct, "/") {
			return l, nil
		}
		p.next()
		r, err := p.exponent()
		if err != nil {
			return nil, err
		}
		l = &binNode{t.text, l, r}
	}
}

func (p *parser) exponent() (node, error) {
	l, err := p.unary()
	if err != nil {
		return nil, err
	}
	for p.accept(tokPunct, "**") {
		r, err := p.unary()
		if err != nil {
			return nil, err
		}
		l = &binNode{"**", l, r}
	}
	return l, nil
}

func (p *parser) unary() (node, error) {
	if p.accept(tokPunct, "-") {
		x, err := p.unary()
		if err != nil {
			return nil, err
		}
		return &negNode{x}, nil
	}
	return p.postfix()
}

func (p *parser) postfix() (node, error) {
	x, err := p.primary()
	if err != nil {
		return nil, err
	}
	for {
		switch {
		case p.accept(tokPunct, "."):
			name := p.next()
			if name.kind != tokName && name.kind != tokString {
				return nil, fmt.Errorf("expected a name after '.' at %d", name.pos)
			}
			x = &pathNode{x, name.text}
		case p.peek().is(tokPunct, "["):
			p.next()
			cond, err := p.expr()
			if err != nil {
				return nil, err
			}
			if err := p.expect(tokPunct, "]"); err != nil {
				return nil, err
			}
			x = &filterNode{x, cond}
		default:
			return x, nil
		}
	}
}

func (p *parser) primary() (node, error) {
	t := p.peek()
	switch t.kind {
	case tokNumber:
		p.next()
		r, ok := new(big.Rat).SetString(t.text)
		if !ok {
			return nil, fmt.Errorf("invalid number %q", t.text)
		}
		return &litNode{r}, nil
	case tokString:
		p.next()
		return &litNode{t.text}, nil
	case tokTemporal:
		p.next()
		v, err := parseTemporal(t.text)
		if err != nil {
			return nil, err
		}
		return &litNode{v}, nil
	case tokName:
		switch t.text {
		case "true", "false":
			p.next()
			return &litNode{t.text == "true"}, nil
		case "null":
			p.next()
			return &litNode{nil}, nil
		case "not":
			// not(...) negates its argument, or the tests in a unary test.
			if p.peekAt(1).is(tokPunct, "(") {
				p.next()
				p.next()
				var tests []node
				for {
					test, err := p.unaryTest()
					if err != nil {
						return nil, err
					}
					tests = append(tests, test)
					if p.accept(tokPunct, ")") {
						return &notTestNode{tests}, nil
					}
					if err := p.expect(tokPunct, ","); err != nil {
						return nil, err
					}
				}
			}
		}
		if name, n := p.functionName(); n > 0 {
			p.pos += n
			return p.call(name)
		}
		p.next()
		return &nameNode{t.text}, nil
	case tokPunct:
		switch t.text {
		case "(":
			if p.looksLikeRange() {
				return p.rangeLiteral()
			}
			p.next()
			x, err := p.expr()
			if err != nil {
				return nil, err
			}
			if err := p.expect(tokPunct, ")"); err != nil {
				return nil, err
			}
			return x, nil
		case "[":
			if p.looksLikeRange() {
				return p.rangeLiteral()
			}
			p.next()
			var items []node
			if p.accept(tokPunct, "]") {
				return &listNode{items}, nil
			}
			for {
				item, err := p.expr()
				if err != nil {
					return nil, err
				}
				items = append(items, item)
				if p.accept(tokPunct, "]") {
					return &listNode{items}, nil
				}
				if err := p.expect(tokPunct, ","); err != nil {
					return nil, err
				}
			}
		case "]":
			// "]1..10]": a range open at the start.
			return p.rangeLiteral()
		case "{":
			return p.context()
		}
	case tokEOF:
		return nil, fmt.Errorf("unexpected end of expression")
	}
	return nil, fmt.Errorf("unexpected %q at %d", t.text, t.pos)
}

// functionName returns the longest built-in or single-word function name
// starting at the cursor and followed by "(", with how many tokens it
// spans; 0 when the cursor isn't at a function call.
func (p *parser) functionName() (string, int) {
	words := []string{}
	best, bestN := "", 0
	for i := 0; i < 5; i++ {
		t := p.peekAt(i)
		if t.kind != tokName {
			break
		}
		words = append(words, t.text)
		name := strings.Join(words, " ")
		if p.peekAt(i+1).is(tokPunct, "(") && (i == 0 || multiWordFunctions[name]) {
			best, bestN = name, i+1
		}
	}
	return best, bestN
}

func (p *parser) call(name string) (node, error) {
	if err := p.expect(tokPunct, "("); err != nil {
		return nil, err
	}
	c := &callNode{fn: name}
	if p.accept(tokPunct, ")") {
		return c, nil
	}
	for {
		// Named argument: name ":" expr, where the name may span several
		// words ("start position: 2").
		if words := p.namedArgumentWords(); words > 0 {
			parts := make([]string, words)
			for i := range parts {
				parts[i] = p.next().text
			}
			argName := strings.Join(parts, " ")
			p.next() // ":"
			v, err := p.expr()
			if err != nil {
				return nil, err
			}
			if c.named == nil {
				c.named = map[string]node{}
			}
			c.named[argName] = v
		} else {
			arg, err := p.expr()
			if err != nil {
				return nil, err
			}
			c.args = append(c.args, arg)
		}
		if p.accept(tokPunct, ")") {
			return c, nil
		}
		if err := p.expect(tokPunct, ","); err != nil {
			return nil, err
		}
	}
}

func (p *parser) rangeLiteral() (node, error) {
	open := p.next().text
	lo, err := p.additive()
	if err != nil {
		return nil, err
	}
	if err := p.expect(tokPunct, ".."); err != nil {
		return nil, err
	}
	hi, err := p.additive()
	if err != nil {
		return nil, err
	}
	closeTok := p.next()
	if closeTok.kind != tokPunct || (closeTok.text != "]" && closeTok.text != ")" && closeTok.text != "[") {
		return nil, fmt.Errorf("expected ']' or ')' to close the range at %d", closeTok.pos)
	}
	return &rangeNode{lo: lo, hi: hi, loClosed: open == "[", hiClosed: closeTok.text == "]"}, nil
}

func (p *parser) context() (node, error) {
	if err := p.expect(tokPunct, "{"); err != nil {
		return nil, err
	}
	c := &contextNode{}
	if p.accept(tokPunct, "}") {
		return c, nil
	}
	for {
		k := p.next()
		if k.kind != tokName && k.kind != tokString {
			return nil, fmt.Errorf("expected a context key at %d", k.pos)
		}
		key := k.text
		// Keys may contain spaces: {order total: 10}.
		for p.peek().kind == tokName {
			key += " " + p.next().text
		}
		if err := p.expect(tokPunct, ":"); err != nil {
			return nil, err
		}
		v, err := p.expr()
		if err != nil {
			return nil, err
		}
		c.keys = append(c.keys, key)
		c.vals = append(c.vals, v)
		if p.accept(tokPunct, "}") {
			return c, nil
		}
		if err := p.expect(tokPunct, ","); err != nil {
			return nil, err
		}
	}
}

// parseUnaryTests parses a DMN input entry: comma-separated unary tests,
// "-", or not(...).
func parseUnaryTests(src string) ([]node, error) {
	toks, err := lex(src)
	if err != nil {
		return nil, err
	}
	p := &parser{toks: toks}
	var tests []node
	for {
		t, err := p.unaryTest()
		if err != nil {
			return nil, err
		}
		tests = append(tests, t)
		if p.peek().kind == tokEOF {
			return tests, nil
		}
		if err := p.expect(tokPunct, ","); err != nil {
			return nil, err
		}
	}
}

// namedArgumentWords returns how many name tokens at the cursor form a
// named argument's name (they're followed by ":"), or 0.
func (p *parser) namedArgumentWords() int {
	for i := 0; i < 4; i++ {
		t := p.peekAt(i)
		if t.is(tokPunct, ":") {
			return i
		}
		if t.kind != tokName {
			return 0
		}
	}
	return 0
}
