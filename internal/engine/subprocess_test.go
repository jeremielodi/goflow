package engine

import (
	"strings"
	"testing"
)

const subProcessBPMN = `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" id="d">
  <bpmn:error id="Err_1" errorCode="PAYMENT_FAILED"/>
  <bpmn:process id="order" isExecutable="true">
    <bpmn:startEvent id="start"/>
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="sub"/>
    <bpmn:subProcess id="sub" name="Payment">
      <bpmn:startEvent id="subStart"/>
      <bpmn:sequenceFlow id="s1" sourceRef="subStart" targetRef="charge"/>
      <bpmn:serviceTask id="charge"/>
      <bpmn:sequenceFlow id="s2" sourceRef="charge" targetRef="inner"/>
      <bpmn:subProcess id="inner">
        <bpmn:startEvent id="innerStart"/>
        <bpmn:sequenceFlow id="i1" sourceRef="innerStart" targetRef="innerEnd"/>
        <bpmn:endEvent id="innerEnd"/>
      </bpmn:subProcess>
      <bpmn:sequenceFlow id="s3" sourceRef="inner" targetRef="subEnd"/>
      <bpmn:endEvent id="subEnd"/>
    </bpmn:subProcess>
    <bpmn:boundaryEvent id="timeout" attachedToRef="sub" cancelActivity="true">
      <bpmn:timerEventDefinition><bpmn:timeDuration>PT1H</bpmn:timeDuration></bpmn:timerEventDefinition>
    </bpmn:boundaryEvent>
    <bpmn:boundaryEvent id="failed" attachedToRef="sub">
      <bpmn:errorEventDefinition errorRef="Err_1"/>
    </bpmn:boundaryEvent>
    <bpmn:sequenceFlow id="f2" sourceRef="sub" targetRef="end"/>
    <bpmn:sequenceFlow id="f3" sourceRef="timeout" targetRef="end"/>
    <bpmn:sequenceFlow id="f4" sourceRef="failed" targetRef="end"/>
    <bpmn:endEvent id="end"/>
  </bpmn:process>
</bpmn:definitions>`

func TestBuildGraph_EmbeddedSubProcess(t *testing.T) {
	g, err := BuildGraphForProcess([]byte(subProcessBPMN), "order")
	if err != nil {
		t.Fatal(err)
	}

	sub := g.Nodes["sub"]
	if sub == nil || sub.Type != SubProcessType || sub.StartNodeID != "subStart" || sub.ParentScope != "" {
		t.Fatalf("unexpected sub-process node: %+v", sub)
	}
	inner := g.Nodes["inner"]
	if inner == nil || inner.Type != SubProcessType || inner.StartNodeID != "innerStart" || inner.ParentScope != "sub" {
		t.Fatalf("unexpected nested sub-process node: %+v", inner)
	}

	wantScope := map[string]string{
		"start": "", "end": "", "timeout": "", "failed": "",
		"subStart": "sub", "charge": "sub", "subEnd": "sub",
		"innerStart": "inner", "innerEnd": "inner",
	}
	for id, scope := range wantScope {
		n := g.Nodes[id]
		if n == nil {
			t.Fatalf("node %s missing", id)
		}
		if n.ParentScope != scope {
			t.Errorf("node %s: ParentScope = %q, want %q", id, n.ParentScope, scope)
		}
	}

	var processStarts []string
	for _, n := range g.Nodes {
		if n.IsProcessStart() {
			processStarts = append(processStarts, n.ID)
		}
	}
	if len(processStarts) != 1 || processStarts[0] != "start" {
		t.Errorf("IsProcessStart matched %v, want only [start]", processStarts)
	}

	if len(sub.Outgoing) == 0 || sub.Outgoing[0].TargetRef == "" {
		t.Errorf("sub-process has no outgoing flow: %+v", sub.Outgoing)
	}
	if g.Nodes["timeout"].Type != BoundaryTimerEventType || g.Nodes["timeout"].AttachedToRef != "sub" {
		t.Errorf("timer boundary not attached to sub-process: %+v", g.Nodes["timeout"])
	}
	if f := g.Nodes["failed"]; f.Type != ErrorBoundaryEventType || f.AttachedToRef != "sub" || f.ErrorCode != "PAYMENT_FAILED" {
		t.Errorf("error boundary not attached to sub-process: %+v", f)
	}
	if next := g.Nodes["charge"].Outgoing; len(next) != 1 || next[0].TargetRef != "inner" {
		t.Errorf("charge should flow into the nested sub-process, got %+v", next)
	}
}

func TestBuildGraph_SubProcessErrors(t *testing.T) {
	cases := map[string]struct{ body, want string }{
		"no start event": {
			`<bpmn:subProcess id="sub"><bpmn:endEvent id="subEnd"/></bpmn:subProcess>`,
			"has no start event",
		},
		"event sub-process": {
			`<bpmn:subProcess id="sub" triggeredByEvent="true"><bpmn:startEvent id="s"/></bpmn:subProcess>`,
			"needs a timer, message, signal, error, escalation or conditional trigger",
		},
		"two start events": {
			`<bpmn:subProcess id="sub"><bpmn:startEvent id="a"/><bpmn:startEvent id="b"/></bpmn:subProcess>`,
			"more than one start event",
		},
	}
	for name, c := range cases {
		xml := `<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" id="d">
		  <bpmn:process id="p" isExecutable="true"><bpmn:startEvent id="start"/>` + c.body + `</bpmn:process></bpmn:definitions>`
		_, err := BuildGraphForProcess([]byte(xml), "p")
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: got error %v, want one containing %q", name, err, c.want)
		}
	}
}

// A process without sub-processes must build exactly as before.
func TestBuildGraph_NoSubProcessUnchanged(t *testing.T) {
	xml := `<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" id="d">
	  <bpmn:process id="p" isExecutable="true">
	    <bpmn:startEvent id="start"/>
	    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="task"/>
	    <bpmn:userTask id="task"/>
	    <bpmn:sequenceFlow id="f2" sourceRef="task" targetRef="end"/>
	    <bpmn:endEvent id="end"/>
	  </bpmn:process></bpmn:definitions>`
	g, err := BuildGraphForProcess([]byte(xml), "p")
	if err != nil {
		t.Fatal(err)
	}
	if len(g.Nodes) != 3 {
		t.Fatalf("got %d nodes, want 3", len(g.Nodes))
	}
	for _, n := range g.Nodes {
		if n.ParentScope != "" || n.StartNodeID != "" {
			t.Errorf("node %s got sub-process fields set: %+v", n.ID, n)
		}
	}
}

const errorCatchBPMN = `<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" id="d">
  <bpmn:error id="E1" errorCode="E1"/>
  <bpmn:error id="E2" errorCode="E2"/>
  <bpmn:error id="E3" errorCode="E3"/>
  <bpmn:message id="M" name="Cancel"/>
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="start"/>
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="outer"/>
    <bpmn:subProcess id="outer">
      <bpmn:startEvent id="o_start"/>
      <bpmn:sequenceFlow id="o1" sourceRef="o_start" targetRef="task"/>
      <bpmn:serviceTask id="task"/>
      <bpmn:sequenceFlow id="o2" sourceRef="task" targetRef="o_end"/>
      <bpmn:endEvent id="o_end"/>
      <bpmn:subProcess id="outer_on_e2" triggeredByEvent="true">
        <bpmn:startEvent id="e2_start"><bpmn:errorEventDefinition errorRef="E2"/></bpmn:startEvent>
        <bpmn:sequenceFlow id="e21" sourceRef="e2_start" targetRef="e2_end"/>
        <bpmn:endEvent id="e2_end"/>
      </bpmn:subProcess>
    </bpmn:subProcess>
    <bpmn:boundaryEvent id="task_e1" attachedToRef="task"><bpmn:errorEventDefinition errorRef="E1"/></bpmn:boundaryEvent>
    <bpmn:boundaryEvent id="outer_e2" attachedToRef="outer"><bpmn:errorEventDefinition errorRef="E2"/></bpmn:boundaryEvent>
    <bpmn:boundaryEvent id="outer_e3" attachedToRef="outer"><bpmn:errorEventDefinition errorRef="E3"/></bpmn:boundaryEvent>
    <bpmn:sequenceFlow id="f2" sourceRef="outer" targetRef="end"/>
    <bpmn:sequenceFlow id="f3" sourceRef="task_e1" targetRef="end"/>
    <bpmn:sequenceFlow id="f4" sourceRef="outer_e2" targetRef="end"/>
    <bpmn:sequenceFlow id="f5" sourceRef="outer_e3" targetRef="end"/>
    <bpmn:endEvent id="end"/>
    <bpmn:subProcess id="on_cancel" triggeredByEvent="true">
      <bpmn:startEvent id="c_start" isInterrupting="false"><bpmn:messageEventDefinition messageRef="M"/></bpmn:startEvent>
      <bpmn:sequenceFlow id="c1" sourceRef="c_start" targetRef="c_end"/>
      <bpmn:endEvent id="c_end"/>
    </bpmn:subProcess>
  </bpmn:process>
</bpmn:definitions>`

func TestFindErrorCatcher_Order(t *testing.T) {
	g, err := BuildGraphForProcess([]byte(errorCatchBPMN), "p")
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		element, code, wantCatcher, wantScope string
	}{
		{"task", "E1", "task_e1", ""},          // boundary on the activity itself
		{"task", "E2", "outer_on_e2", "outer"}, // event sub-process inside the scope beats the boundary on it
		{"task", "E3", "outer_e3", "outer"},    // then the boundary on the enclosing sub-process
		{"task", "NOPE", "", ""},               // nothing catches it
		{"e2_end", "E2", "outer_e2", "outer"},  // an event sub-process doesn't catch its own error
	}
	for _, c := range cases {
		got, scope := g.FindErrorCatcher(c.element, c.code)
		gotID := ""
		if got != nil {
			gotID = got.ID
		}
		if gotID != c.wantCatcher || scope != c.wantScope {
			t.Errorf("FindErrorCatcher(%s, %s) = (%q, %q), want (%q, %q)", c.element, c.code, gotID, scope, c.wantCatcher, c.wantScope)
		}
	}
}

func TestBuildGraph_EventSubProcess(t *testing.T) {
	g, err := BuildGraphForProcess([]byte(errorCatchBPMN), "p")
	if err != nil {
		t.Fatal(err)
	}
	c := g.Nodes["on_cancel"]
	if c.Type != EventSubProcessType || c.TriggerType != TriggerMessage || c.MessageRef != "Cancel" || c.CancelActivity || c.StartNodeID != "c_start" || c.ParentScope != "" {
		t.Errorf("unexpected message event sub-process node: %+v", c)
	}
	e := g.Nodes["outer_on_e2"]
	if e.Type != EventSubProcessType || e.TriggerType != TriggerError || e.ErrorCode != "E2" || !e.CancelActivity || e.ParentScope != "outer" {
		t.Errorf("unexpected error event sub-process node: %+v", e)
	}
	if got := g.TriggerNodes(); len(got) != 2 || !got["on_cancel"] || !got["outer_on_e2"] {
		t.Errorf("TriggerNodes() = %v", got)
	}
	for _, n := range g.Nodes {
		if n.IsProcessStart() && n.ID != "start" {
			t.Errorf("%s inside an event sub-process counts as the process start", n.ID)
		}
	}
}

// Camunda Modeler omits cancelActivity on interrupting boundary events: the
// BPMN default (true) must apply.
func TestBuildGraph_BoundaryDefaultsToInterrupting(t *testing.T) {
	xml := `<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" id="d">
	  <bpmn:message id="M" name="Go"/>
	  <bpmn:process id="p" isExecutable="true">
	    <bpmn:startEvent id="s"/>
	    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="t"/>
	    <bpmn:userTask id="t"/>
	    <bpmn:boundaryEvent id="dflt" attachedToRef="t"><bpmn:messageEventDefinition messageRef="M"/></bpmn:boundaryEvent>
	    <bpmn:boundaryEvent id="nonint" attachedToRef="t" cancelActivity="false">
	      <bpmn:timerEventDefinition><bpmn:timeDuration>PT1M</bpmn:timeDuration></bpmn:timerEventDefinition>
	    </bpmn:boundaryEvent>
	    <bpmn:sequenceFlow id="f2" sourceRef="t" targetRef="e"/>
	    <bpmn:sequenceFlow id="f3" sourceRef="dflt" targetRef="e"/>
	    <bpmn:sequenceFlow id="f4" sourceRef="nonint" targetRef="e"/>
	    <bpmn:endEvent id="e"/>
	  </bpmn:process></bpmn:definitions>`
	g, err := BuildGraphForProcess([]byte(xml), "p")
	if err != nil {
		t.Fatal(err)
	}
	if !g.Nodes["dflt"].CancelActivity {
		t.Error("a boundary event without cancelActivity must be interrupting")
	}
	if g.Nodes["nonint"].CancelActivity {
		t.Error(`cancelActivity="false" must be non-interrupting`)
	}
}

func TestFindEscalationCatcher(t *testing.T) {
	xml := `<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" id="d">
	  <bpmn:escalation id="E1" escalationCode="LATE"/>
	  <bpmn:escalation id="E2" escalationCode="NOTE"/>
	  <bpmn:process id="p" isExecutable="true">
	    <bpmn:startEvent id="s"/>
	    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="sub"/>
	    <bpmn:subProcess id="sub">
	      <bpmn:startEvent id="ss"/>
	      <bpmn:sequenceFlow id="i1" sourceRef="ss" targetRef="throwLate"/>
	      <bpmn:intermediateThrowEvent id="throwLate"><bpmn:escalationEventDefinition escalationRef="E1"/></bpmn:intermediateThrowEvent>
	      <bpmn:sequenceFlow id="i2" sourceRef="throwLate" targetRef="throwNote"/>
	      <bpmn:intermediateThrowEvent id="throwNote"><bpmn:escalationEventDefinition escalationRef="E2"/></bpmn:intermediateThrowEvent>
	      <bpmn:sequenceFlow id="i3" sourceRef="throwNote" targetRef="se"/>
	      <bpmn:endEvent id="se"/>
	    </bpmn:subProcess>
	    <bpmn:boundaryEvent id="catchLate" attachedToRef="sub"><bpmn:escalationEventDefinition escalationRef="E1"/></bpmn:boundaryEvent>
	    <bpmn:boundaryEvent id="catchAll" attachedToRef="sub" cancelActivity="false"><bpmn:escalationEventDefinition/></bpmn:boundaryEvent>
	    <bpmn:sequenceFlow id="f2" sourceRef="sub" targetRef="e"/>
	    <bpmn:sequenceFlow id="f3" sourceRef="catchLate" targetRef="e"/>
	    <bpmn:sequenceFlow id="f4" sourceRef="catchAll" targetRef="e"/>
	    <bpmn:endEvent id="e"/>
	  </bpmn:process></bpmn:definitions>`
	g, err := BuildGraphForProcess([]byte(xml), "p")
	if err != nil {
		t.Fatal(err)
	}
	if c, scope := g.FindEscalationCatcher("throwLate", "LATE"); c == nil || c.ID != "catchLate" || scope != "sub" {
		t.Errorf("LATE should be caught by catchLate (exact code beats catch-all), got %v", c)
	}
	if c, _ := g.FindEscalationCatcher("throwNote", "NOTE"); c == nil || c.ID != "catchAll" || c.CancelActivity {
		t.Errorf("NOTE should be caught by the non-interrupting catch-all, got %+v", c)
	}
	if c, _ := g.FindEscalationCatcher("e", "LATE"); c != nil {
		t.Errorf("nothing outside the sub-process catches, got %v", c.ID)
	}
}

func TestBuildGraph_CompensationHandlers(t *testing.T) {
	xml := `<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" id="d">
	  <bpmn:process id="p" isExecutable="true">
	    <bpmn:startEvent id="s"/>
	    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="book"/>
	    <bpmn:userTask id="book"/>
	    <bpmn:boundaryEvent id="bookComp" attachedToRef="book"><bpmn:compensateEventDefinition/></bpmn:boundaryEvent>
	    <bpmn:userTask id="unbook" isForCompensation="true"/>
	    <bpmn:association id="a1" sourceRef="bookComp" targetRef="unbook"/>
	    <bpmn:sequenceFlow id="f2" sourceRef="book" targetRef="undo"/>
	    <bpmn:intermediateThrowEvent id="undo"><bpmn:compensateEventDefinition activityRef="book"/></bpmn:intermediateThrowEvent>
	    <bpmn:sequenceFlow id="f3" sourceRef="undo" targetRef="e"/>
	    <bpmn:endEvent id="e"/>
	  </bpmn:process></bpmn:definitions>`
	g, err := BuildGraphForProcess([]byte(xml), "p")
	if err != nil {
		t.Fatal(err)
	}
	if g.Nodes["book"].CompensationHandler != "unbook" {
		t.Errorf("book's handler = %q, want unbook", g.Nodes["book"].CompensationHandler)
	}
	if out := g.Nodes["unbook"].Outgoing; len(out) != 1 || g.Nodes[out[0].TargetRef].Type != CompensationHandlerEndType {
		t.Errorf("handler should flow to a synthetic end node, got %+v", out)
	}
	if u := g.Nodes["undo"]; u.Type != IntermediateCompensationThrowEventType || u.CompensateActivity != "book" {
		t.Errorf("unexpected throw node: %+v", u)
	}

	missing := strings.Replace(xml, `<bpmn:association id="a1" sourceRef="bookComp" targetRef="unbook"/>`, "", 1)
	if _, err := BuildGraphForProcess([]byte(missing), "p"); err == nil || !strings.Contains(err.Error(), "no associated compensation handler") {
		t.Errorf("a compensation boundary without handler should be rejected, got %v", err)
	}
}
