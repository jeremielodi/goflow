package engine

// FindErrorCatcher returns what catches BPMN error errorCode thrown at
// elementID, searching like BPMN does: an error boundary event attached to
// the element itself, then, scope by scope going outward, an error-start
// event sub-process inside the scope, then an error boundary event attached
// to the enclosing sub-process.
//
// The catcher is an ErrorBoundaryEventType or EventSubProcessType node, or
// nil when nothing catches the error. scope is the sub-process the catcher
// belongs to — the one its boundary is attached to, or the one its event
// sub-process sits in — and "" for a boundary on elementID itself or for a
// process-level event sub-process.
func (g *ProcessGraph) FindErrorCatcher(elementID, errorCode string) (catcher *Node, scope string) {
	if b := g.errorBoundary(elementID, errorCode); b != nil {
		return b, ""
	}

	from := elementID
	scope = ""
	if n, ok := g.Nodes[elementID]; ok {
		scope = n.ParentScope
	}
	for {
		// An event sub-process never catches errors it throws itself.
		if es := g.errorEventSubProcess(scope, errorCode, from); es != nil {
			return es, scope
		}
		if scope == "" {
			return nil, ""
		}
		if b := g.errorBoundary(scope, errorCode); b != nil {
			return b, scope
		}
		owner, ok := g.Nodes[scope]
		if !ok {
			return nil, ""
		}
		from, scope = scope, owner.ParentScope
	}
}

func (g *ProcessGraph) errorBoundary(attachedTo, errorCode string) *Node {
	for _, n := range g.Nodes {
		if n.Type == ErrorBoundaryEventType && n.AttachedToRef == attachedTo && n.ErrorCode == errorCode {
			return n
		}
	}
	return nil
}

func (g *ProcessGraph) errorEventSubProcess(scope, errorCode, exclude string) *Node {
	for _, n := range g.Nodes {
		if n.Type == EventSubProcessType && n.TriggerType == TriggerError &&
			n.ParentScope == scope && n.ErrorCode == errorCode && n.ID != exclude {
			return n
		}
	}
	return nil
}

// TriggerNodes returns the IDs of every event sub-process node and every
// timer/message/signal boundary event node. Executions waiting on one are
// armed triggers waiting for their event, not running work, so they don't
// keep a scope or the process alive.
func (g *ProcessGraph) TriggerNodes() map[string]bool {
	ids := map[string]bool{}
	for id, n := range g.Nodes {
		switch n.Type {
		case EventSubProcessType, BoundaryTimerEventType, MessageBoundaryEventType, SignalBoundaryEventType, ConditionalBoundaryEventType:
			ids[id] = true
		}
	}
	return ids
}

// FindEscalationCatcher returns what catches escalation code thrown at
// elementID (an intermediate or end escalation event): scope by scope
// going outward, an escalation event sub-process inside the scope, then an
// escalation boundary event attached to the enclosing sub-process. A catch
// event without a code catches every escalation. scope is the sub-process
// the catcher belongs to ("" for a process-level event sub-process); nil
// when nothing catches it — then the token simply carries on.
func (g *ProcessGraph) FindEscalationCatcher(elementID, code string) (catcher *Node, scope string) {
	// At one level, a catch event for this exact code wins over a
	// catch-all one (no code).
	find := func(ok func(*Node) bool) *Node {
		var catchAll *Node
		for _, n := range g.Nodes {
			if !ok(n) {
				continue
			}
			if n.EscalationCode == code {
				return n
			}
			if n.EscalationCode == "" {
				catchAll = n
			}
		}
		return catchAll
	}

	from := elementID
	if n, ok := g.Nodes[elementID]; ok {
		scope = n.ParentScope
	}
	for {
		if es := find(func(n *Node) bool {
			return n.Type == EventSubProcessType && n.TriggerType == TriggerEscalation && n.ParentScope == scope && n.ID != from
		}); es != nil {
			return es, scope
		}
		if scope == "" {
			return nil, ""
		}
		if b := find(func(n *Node) bool {
			return n.Type == EscalationBoundaryEventType && n.AttachedToRef == scope
		}); b != nil {
			return b, scope
		}
		owner, ok := g.Nodes[scope]
		if !ok {
			return nil, ""
		}
		from, scope = scope, owner.ParentScope
	}
}
