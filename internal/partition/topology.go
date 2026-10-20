package partition

import (
	"context"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

// Topology describes the cluster the way Zeebe's Topology does: each
// GoFlow replica is a broker, leading the partitions it owns.
type Topology struct {
	Partitioned       bool
	PartitionsCount   int
	ReplicationFactor int
	Brokers           []Broker
}

// Broker is one replica and the partitions (numbered from 1, as in Zeebe)
// it owns.
type Broker struct {
	Host       string
	Partitions []int
}

var (
	topoMu      sync.RWMutex
	topoBrokers []string
	topoCount   int
	topoRF      int
)

// SetTopologySource records what Describe needs: Kafka's view of who
// owns which partition. Not called in direct mode.
func SetTopologySource(brokers []string, partitions, replicationFactor int) {
	topoMu.Lock()
	defer topoMu.Unlock()
	topoBrokers, topoCount, topoRF = brokers, partitions, replicationFactor
}

// clientID names this replica in the consumer group (its host name), so
// the topology can tell replicas apart.
func clientID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		return "goflow"
	}
	return host
}

// Describe returns the current topology. In direct mode the replica
// answering is the whole cluster, with one partition.
func Describe(ctx context.Context) (Topology, error) {
	topoMu.RLock()
	brokers, count, rf := topoBrokers, topoCount, topoRF
	topoMu.RUnlock()
	if len(brokers) == 0 {
		return Topology{PartitionsCount: 1, ReplicationFactor: 1, Brokers: []Broker{{Host: clientID(), Partitions: []int{1}}}}, nil
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	client := &kafka.Client{Addr: kafka.TCP(brokers...)}
	resp, err := client.DescribeGroups(ctx, &kafka.DescribeGroupsRequest{GroupIDs: []string{GroupID}})
	if err != nil {
		return Topology{}, fmt.Errorf("describe partition owners: %w", err)
	}
	topo := Topology{Partitioned: true, PartitionsCount: count, ReplicationFactor: rf}
	for _, g := range resp.Groups {
		if g.Error != nil {
			return Topology{}, fmt.Errorf("describe partition owners: %w", g.Error)
		}
		for _, m := range g.Members {
			b := Broker{Host: m.ClientID}
			for _, t := range m.MemberAssignments.Topics {
				if t.Topic != CommandsTopic {
					continue
				}
				for _, p := range t.Partitions {
					b.Partitions = append(b.Partitions, p+1)
				}
			}
			sort.Ints(b.Partitions)
			topo.Brokers = append(topo.Brokers, b)
		}
	}
	sort.Slice(topo.Brokers, func(i, j int) bool { return topo.Brokers[i].Host < topo.Brokers[j].Host })
	return topo, nil
}
