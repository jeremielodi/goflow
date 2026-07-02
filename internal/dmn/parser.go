// Package dmn parses and evaluates DMN 1.3 decision tables — the piece
// Camunda 8 uses for Business Rule Tasks (zeebe:calledDecision) and the
// standalone POST /v2/decisions/:key/evaluation endpoint.
package dmn

import "encoding/xml"

type Definitions struct {
	XMLName   xml.Name   `xml:"definitions"`
	ID        string     `xml:"id,attr" json:"id"`
	Name      string     `xml:"name,attr" json:"name"`
	Decisions []Decision `xml:"decision" json:"decisions"`
}

type Decision struct {
	ID            string         `xml:"id,attr" json:"id"`
	Name          string         `xml:"name,attr" json:"name"`
	DecisionTable *DecisionTable `xml:"decisionTable" json:"decisionTable,omitempty"`
}

// DecisionTable is the parsed, storable representation of a DMN decision
// table: one row per Input/Output column, one Rule per row of the table.
type DecisionTable struct {
	ID        string   `xml:"id,attr" json:"id"`
	HitPolicy string   `xml:"hitPolicy,attr" json:"hitPolicy"`
	Inputs    []Input  `xml:"input" json:"inputs"`
	Outputs   []Output `xml:"output" json:"outputs"`
	Rules     []Rule   `xml:"rule" json:"rules"`
}

type Input struct {
	ID              string `xml:"id,attr" json:"id"`
	Label           string `xml:"label,attr" json:"label"`
	InputExpression struct {
		Text string `xml:"text" json:"text"`
	} `xml:"inputExpression" json:"inputExpression"`
}

type Output struct {
	ID    string `xml:"id,attr" json:"id"`
	Label string `xml:"label,attr" json:"label"`
	Name  string `xml:"name,attr" json:"name"`
}

type Rule struct {
	ID            string  `xml:"id,attr" json:"id"`
	InputEntries  []Entry `xml:"inputEntry" json:"inputEntries"`
	OutputEntries []Entry `xml:"outputEntry" json:"outputEntries"`
}

type Entry struct {
	Text string `xml:"text" json:"text"`
}

// ParseDMN parses a DMN 1.3 XML document.
func ParseDMN(data []byte) (*Definitions, error) {
	var defs Definitions
	if err := xml.Unmarshal(data, &defs); err != nil {
		return nil, err
	}
	return &defs, nil
}
