package v2

import (
	"github.com/gofiber/fiber/v2"

	"github.com/jeremielodi/goflow/internal/partition"
)

// GetTopology handles GET /v2/topology (Camunda 8): the replicas of the
// cluster as brokers, each leading the partitions it owns.
func GetTopology(c *fiber.Ctx) error {
	topo, err := partition.Describe(c.Context())
	if err != nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"title": "UNAVAILABLE", "detail": err.Error()})
	}
	brokers := make([]fiber.Map, 0, len(topo.Brokers))
	for i, b := range topo.Brokers {
		partitions := make([]fiber.Map, 0, len(b.Partitions))
		for _, p := range b.Partitions {
			partitions = append(partitions, fiber.Map{"partitionId": p, "role": "leader", "health": "healthy"})
		}
		brokers = append(brokers, fiber.Map{
			"nodeId":     i,
			"host":       b.Host,
			"port":       26500,
			"partitions": partitions,
			"version":    "goflow-dev",
		})
	}
	writePath := "direct"
	if topo.Partitioned {
		writePath = "partitioned"
	}
	return c.JSON(fiber.Map{
		"brokers":           brokers,
		"clusterSize":       len(topo.Brokers),
		"partitionsCount":   topo.PartitionsCount,
		"replicationFactor": topo.ReplicationFactor,
		"gatewayVersion":    "goflow-dev",
		"writePath":         writePath,
	})
}
