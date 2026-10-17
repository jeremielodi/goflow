package engine

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"strings"
)

// MultiInstanceConfig is an activity's multi-instance loop. Any activity
// can have one: every task type, an embedded sub-process, a call activity.
//
// Camunda 8 (zeebe:loopCharacteristics) and Camunda 7 forms are both read
// and normalized into InputCollection/InputElement/OutputCollection/
// OutputElement; the older fields stay for graphs deployed before them
// (see the accessors).
type MultiInstanceConfig struct {
	IsSequential        bool   `json:"isSequential"`
	LoopCardinality     string `json:"loopCardinality"`
	CompletionCondition string `json:"completionCondition"`

	// InputCollection is a FEEL expression ("=items") or, from older
	// models, a variable name; one inner instance per element.
	InputCollection string `json:"inputCollection,omitempty"`
	// InputElement names the local variable holding an instance's element.
	InputElement string `json:"inputElement,omitempty"`
	// OutputCollection names the variable collecting OutputElement, the
	// FEEL expression evaluated in each inner instance when it completes.
	OutputCollection string `json:"outputCollection,omitempty"`
	OutputElement    string `json:"outputElement,omitempty"`

	// Graphs deployed before the fields above.
	InputDataItem   string `json:"inputDataItem"`
	OutputDataItem  string `json:"outputDataItem"`
	ElementVariable string `json:"elementVariable"`
}

// Collection is the input collection expression or variable name.
func (m *MultiInstanceConfig) Collection() string {
	if m.InputCollection != "" {
		return m.InputCollection
	}
	return m.InputDataItem
}

// Element is the name of the local variable holding an instance's element.
func (m *MultiInstanceConfig) Element() string {
	if m.InputElement != "" {
		return m.InputElement
	}
	return m.ElementVariable
}

// Output is the output collection variable name.
func (m *MultiInstanceConfig) Output() string {
	if m.OutputCollection != "" {
		return m.OutputCollection
	}
	return m.OutputDataItem
}

type multiInstanceXML struct {
	IsSequential        string `xml:"isSequential,attr"`
	LoopCardinality     string `xml:"loopCardinality"`
	CompletionCondition string `xml:"completionCondition"`
	InputDataItem       string `xml:"inputDataItem"`
	OutputDataItem      string `xml:"outputDataItem"`
	// camunda:collection / camunda:elementVariable (any namespace).
	Collection      string `xml:"collection,attr"`
	ElementVariable string `xml:"elementVariable,attr"`
	Ext             *struct {
		Loop *struct {
			InputCollection  string `xml:"inputCollection,attr"`
			InputElement     string `xml:"inputElement,attr"`
			OutputCollection string `xml:"outputCollection,attr"`
			OutputElement    string `xml:"outputElement,attr"`
		} `xml:"loopCharacteristics"`
	} `xml:"extensionElements"`
}

// stripJUEL turns a Camunda 7 "${expr}" into "expr".
func stripJUEL(s string) string {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "${") && strings.HasSuffix(s, "}") {
		return strings.TrimSpace(s[2 : len(s)-1])
	}
	return s
}

func (x *multiInstanceXML) config() *MultiInstanceConfig {
	m := &MultiInstanceConfig{
		IsSequential:        strings.EqualFold(strings.TrimSpace(x.IsSequential), "true"),
		LoopCardinality:     stripJUEL(x.LoopCardinality),
		CompletionCondition: stripJUEL(x.CompletionCondition),
		InputDataItem:       strings.TrimSpace(x.InputDataItem),
		OutputDataItem:      strings.TrimSpace(x.OutputDataItem),
		ElementVariable:     strings.TrimSpace(x.ElementVariable),
	}
	if x.Ext != nil && x.Ext.Loop != nil {
		l := x.Ext.Loop
		m.InputCollection = strings.TrimSpace(l.InputCollection)
		m.InputElement = strings.TrimSpace(l.InputElement)
		m.OutputCollection = strings.TrimSpace(l.OutputCollection)
		m.OutputElement = strings.TrimSpace(l.OutputElement)
	}
	if m.InputCollection == "" && x.Collection != "" {
		m.InputCollection = stripJUEL(x.Collection)
	}
	return m
}

// applyMultiInstance reads every multiInstanceLoopCharacteristics in the
// BPMN and sets it on the graph node of the activity it belongs to.
func applyMultiInstance(bpmnBytes []byte, graph *ProcessGraph) error {
	d := xml.NewDecoder(bytes.NewReader(bpmnBytes))
	var ids []string // element ids, innermost last ("" for elements without one)
	for {
		tok, err := d.Token()
		if err != nil {
			break // io.EOF, or a syntax error the parser already reported
		}
		switch t := tok.(type) {
		case xml.StartElement:
			if t.Name.Local == "multiInstanceLoopCharacteristics" {
				var x multiInstanceXML
				if err := d.DecodeElement(&x, &t); err != nil {
					return fmt.Errorf("multi-instance loop: %w", err)
				}
				for i := len(ids) - 1; i >= 0; i-- {
					if ids[i] == "" {
						continue
					}
					if node, ok := graph.Nodes[ids[i]]; ok {
						node.MultiInstance = x.config()
					}
					break
				}
				continue
			}
			id := ""
			for _, a := range t.Attr {
				if a.Name.Local == "id" {
					id = a.Value
				}
			}
			ids = append(ids, id)
		case xml.EndElement:
			if len(ids) > 0 {
				ids = ids[:len(ids)-1]
			}
		}
	}
	return nil
}
