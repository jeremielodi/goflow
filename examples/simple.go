package main

import (
	"fmt"
	"os"

	"github.com/jeremielodi/goflow/internal/common"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/parser"
	"github.com/jeremielodi/goflow/internal/runtime"
)

func main1() {

	data, err := os.ReadFile("./examples/simple.bpmn")
	if err != nil {
		panic(err)
	}

	defs, err := parser.ParseBPMN(data)
	if err != nil {
		panic(err)
	}

	graph, err := engine.BuildGraph(defs)
	if err != nil {
		panic(err)
	}

	rt := runtime.Runtime{
		Graph: graph,
	}

	fmt.Println("Starting process")

	// =========================
	// GLOBAL VARIABLES (process context)
	// =========================
	variables := map[string]interface{}{
		"approved": true, // change this to test both paths
		"amount":   1500,
		"user":     "jeremie",
	}

	// =========================
	// STEP 1: START PROCESS
	// =========================
	next, err := rt.Execute("StartEvent_1", variables)
	if err != nil {
		fmt.Println("Execution error:", err)
		return
	}

	current := next

	// =========================
	// STEP 2: CONTINUE LOOP UNTIL END
	// =========================
	for current != "" {

		node, ok := graph.Nodes[current]
		if !ok {
			fmt.Println("Node not found:", current)
			return
		}

		fmt.Printf("Executing: %s (%s)\n", node.ID, node.Type)

		switch node.Type {

		// -------------------------
		// USER TASK 1
		// -------------------------
		case engine.UserTaskType:
			fmt.Printf("UserTask '%s' – waiting for human action\n", node.Name)

			fmt.Println("User completes task:", node.ID)

			// continue flow
			current = node.Outgoing[0].TargetRef

		// -------------------------
		// SERVICE TASK
		// -------------------------
		case engine.ServiceTaskType:
			fmt.Printf("Executing service task: %s\n", node.Name)

			next, err := common.ResolveNext(node, variables)
			if err != nil {
				fmt.Println("Error:", err)
				return
			}

			current = next

		// -------------------------
		// EXCLUSIVE GATEWAY
		// -------------------------
		case engine.ExclusiveGatewayType:
			fmt.Println("Evaluating gateway...")

			next, err := common.ResolveNext(node, variables)
			if err != nil {
				fmt.Println("Gateway error:", err)
				return
			}

			current = next

		// -------------------------
		// END
		// -------------------------
		case engine.EndEventType:
			fmt.Println("PROCESS COMPLETED ✔")
			return

		// -------------------------
		// DEFAULT
		// -------------------------
		default:
			fmt.Println("Unknown node type:", node.Type)
			return
		}
	}
}
