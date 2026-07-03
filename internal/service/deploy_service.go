// internal/service/deploy_service.go
package service

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/dmn"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jeremielodi/goflow/internal/parser"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jmoiron/sqlx"
)

// ErrDeployPermissionDenied lets callers (REST controllers, the gRPC
// gateway) distinguish a permission failure from other deploy errors via
// errors.Is, while still getting a human-readable message from err.Error().
var ErrDeployPermissionDenied = fmt.Errorf("deploy permission denied")

// RawResource is a single deployable file, decoupled from the HTTP
// multipart form it usually arrives in — the gRPC gateway's
// DeployResource RPC already hands over {name, content} pairs directly.
type RawResource struct {
	Filename string
	Content  []byte
}

type DeployedProcessDefinition struct {
	ID           uuid.UUID
	Key          string
	Version      int
	ResourceName string
}

type DeployedDecision struct {
	ID           uuid.UUID
	Key          string
	Version      int
	ResourceName string
}

type DeployedForm struct {
	ID           uuid.UUID
	Key          string
	Version      int
	ResourceName string
}

type DeployResult struct {
	DeploymentID       uuid.UUID
	DeploymentName     string
	ProcessDefinitions []DeployedProcessDefinition
	Decisions          []DeployedDecision
	Forms              []DeployedForm
}

// DeployResources deploys any mix of .bpmn/.dmn/.form resources in one
// deployment record — extracted from what used to be the entire inline
// body of internal/api/process_definitions.go's DeployBPMN handler (the
// one deploy-shaped gap left after StartProcessInstance was consolidated
// the same way). requestingUserID gates deploying a NEW version of an
// EXISTING, restricted process key via CanAccessProcess(..., "MANAGE");
// deploying a brand-new key still requires the global
// CAN_MANAGE_DEPLOY_PROCESS action directly, since CanAccessProcess would
// otherwise trivially allow it (an unrestricted/nonexistent key has no
// grants to check).
func DeployResources(db *sqlx.DB, deploymentName string, tenantID string, resources []RawResource, requestingUserID uuid.UUID) (*DeployResult, error) {
	deploymentRepo := repository.NewProcessRepository(db)
	dmnRepo := repository.NewDMNRepository(db)
	formRepo := repository.NewFormRepository(db)
	actionRepo := repository.NewActionRepository(db)

	if _, err := deploymentRepo.CreateDeployment(models.DeploymentCreateModel{
		Name:       deploymentName,
		DeployedBy: nil,
		Status:     "active",
	}); err != nil {
		return nil, fmt.Errorf("failed to create deployment: %w", err)
	}

	deploy, err := deploymentRepo.FindDeploymentByName(deploymentName)
	if err != nil {
		return nil, fmt.Errorf("deployment created but not found: %w", err)
	}

	result := &DeployResult{DeploymentID: deploy.ID, DeploymentName: deploymentName}

	for _, res := range resources {
		lowerName := strings.ToLower(res.Filename)

		if strings.HasSuffix(lowerName, ".form") {
			var schema struct {
				ID string `json:"id"`
			}
			if err := json.Unmarshal(res.Content, &schema); err != nil || schema.ID == "" {
				return nil, fmt.Errorf("failed to parse form %s: expected JSON with an \"id\" field", res.Filename)
			}
			formID, version, err := formRepo.CreateForm(models.FormCreateModel{
				DeploymentID: deploy.ID,
				FormId:       schema.ID,
				Schema:       res.Content,
			})
			if err != nil {
				return nil, fmt.Errorf("failed to store form %s: %w", schema.ID, err)
			}
			result.Forms = append(result.Forms, DeployedForm{ID: formID, Key: schema.ID, Version: version, ResourceName: res.Filename})
			continue
		}

		if strings.HasSuffix(lowerName, ".dmn") {
			defs, err := dmn.ParseDMN(res.Content)
			if err != nil {
				return nil, fmt.Errorf("failed to parse DMN %s: %w", res.Filename, err)
			}
			for _, decision := range defs.Decisions {
				if decision.DecisionTable == nil {
					continue
				}
				decisionJSON, err := json.Marshal(decision)
				if err != nil {
					return nil, fmt.Errorf("failed to marshal decision %s", decision.ID)
				}
				decisionID, version, err := dmnRepo.CreateDecision(models.DMNDecisionCreateModel{
					DeploymentID: deploy.ID,
					DecisionKey:  decision.ID,
					DecisionName: &decision.Name,
					DmnXML:       string(res.Content),
					ParsedTable:  decisionJSON,
				})
				if err != nil {
					return nil, fmt.Errorf("failed to store decision %s: %w", decision.ID, err)
				}
				result.Decisions = append(result.Decisions, DeployedDecision{ID: decisionID, Key: decision.ID, Version: version, ResourceName: res.Filename})
			}
			continue
		}

		// ---- BPMN resource ----
		bpmnBytes := res.Content
		engineType := parser.DetectEngine(bpmnBytes)

		processKeys, err := extractProcessKeys(bpmnBytes)
		if err != nil {
			return nil, err
		}

		if requestingUserID != uuid.Nil {
			for _, procKey := range processKeys {
				if _, findErr := deploymentRepo.FindLatestProcessDefinitionByKey(procKey); findErr != nil {
					hasGlobal, _ := actionRepo.UserHasPermission(requestingUserID, "CAN_MANAGE_DEPLOY_PROCESS")
					if !hasGlobal {
						return nil, fmt.Errorf("%w: you do not have permission to deploy new process %s", ErrDeployPermissionDenied, procKey)
					}
					continue
				}
				allowed, permErr := CanAccessProcess(db, requestingUserID, procKey, "MANAGE")
				if permErr == nil && !allowed {
					return nil, fmt.Errorf("%w: you do not have MANAGE access to process %s", ErrDeployPermissionDenied, procKey)
				}
			}
		}

		for _, procKey := range processKeys {
			graph, err := engine.BuildGraphForProcess(bpmnBytes, procKey)
			if err != nil {
				return nil, fmt.Errorf("failed to build graph for process %s: %w", procKey, err)
			}

			graphJSON, err := json.Marshal(graph)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal graph for %s", procKey)
			}

			defID, version, err := deploymentRepo.CreateProcessDefinition(models.ProcessDefinitionCreateModel{
				DeploymentID: deploy.ID,
				ProcessKey:   procKey,
				ProcessName:  &procKey,
				IsActive:     true,
				BpmnXML:      string(bpmnBytes),
				ParsedGraph:  graphJSON,
				TenantID:     &tenantID,
				EngineType:   string(engineType),
			})
			if err != nil {
				return nil, fmt.Errorf("failed to store definition %s: %w", procKey, err)
			}

			defUUID, err := uuid.Parse(defID)
			if err != nil {
				return nil, fmt.Errorf("invalid definition id returned for %s: %w", procKey, err)
			}

			result.ProcessDefinitions = append(result.ProcessDefinitions, DeployedProcessDefinition{
				ID: defUUID, Key: procKey, Version: version, ResourceName: fmt.Sprintf("%s.bpmn", procKey),
			})
		}
	}

	return result, nil
}

func extractProcessKeys(xmlBytes []byte) ([]string, error) {
	type Process struct {
		ID string `xml:"id,attr"`
	}
	type Definitions struct {
		Processes []Process `xml:"process"`
	}
	var def Definitions
	if err := xml.Unmarshal(xmlBytes, &def); err != nil {
		return nil, err
	}
	if len(def.Processes) == 0 {
		return nil, fmt.Errorf("no process elements found in BPMN")
	}
	keys := make([]string, len(def.Processes))
	for i, p := range def.Processes {
		keys[i] = p.ID
	}
	return keys, nil
}
