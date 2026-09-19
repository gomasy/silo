// Copyright (c) 2026 PGSTY
// SPDX-License-Identifier: AGPL-3.0-only

package cmd

import (
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/minio/minio/internal/bucket/replication"
	"github.com/prometheus/client_golang/prometheus"
)

// A full MRF queue and an exhausted retry budget drop queue entries, not the
// source objects. Both drops must remain visible in the admin API snapshots.
func TestReplicationMRFDropsVisible(t *testing.T) {
	stats := NewReplicationStats(t.Context(), nil)
	old := globalReplicationStats.Swap(stats)
	t.Cleanup(func() { globalReplicationStats.Store(old) })
	p := &ReplicationPool{
		objLayer:  &replicationMRFTestObjectLayer{},
		stats:     stats,
		mrfSaveCh: make(chan MRFReplicateEntry, 1),
		mrfStopCh: make(chan struct{}),
	}
	p.queueMRFSave(MRFReplicateEntry{sz: 10})
	p.queueMRFSave(MRFReplicateEntry{sz: 20})
	p.queueMRFSave(MRFReplicateEntry{sz: 30, RetryCount: mrfRetryLimit + 1})
	if len(p.mrfSaveCh) != 1 {
		t.Fatalf("queue length = %d, want 1", len(p.mrfSaveCh))
	}
	if got := atomic.LoadUint64(&stats.mrfStats.TotalDroppedCount); got != 2 {
		t.Fatalf("dropped entries = %d, want 2", got)
	}
	for name, qs := range map[string]ReplQNodeStats{
		"bucket": stats.getNodeQueueStats("test-bucket"),
		"node":   stats.getNodeQueueStatsSummary(),
	} {
		t.Run(name, func(t *testing.T) {
			data, err := json.Marshal(qs)
			if err != nil {
				t.Fatal(err)
			}
			var decoded ReplQNodeStats
			if err := json.Unmarshal(data, &decoded); err != nil {
				t.Fatal(err)
			}
			if got := decoded.MRFStats; got.TotalDroppedCount != 2 || got.TotalDroppedBytes != 50 {
				t.Fatalf("API MRF stats = %+v, want dropped count 2 and bytes 50", got)
			}
		})
	}
	oldTargets := globalBucketTargetSys
	globalBucketTargetSys = &BucketTargetSys{}
	t.Cleanup(func() { globalBucketTargetSys = oldTargets })
	want := map[string]float64{"mrf_dropped_operations_total": 2, "mrf_dropped_bytes_total": 50}
	seen := make(map[string]bool)
	for _, metric := range getReplicationNodeMetrics(MetricsGroupOpts{}).Get() {
		name := string(metric.Description.Name)
		if value, ok := want[name]; ok {
			seen[name] = true
			if metric.Value != value || metric.Description.Type != counterMetric {
				t.Errorf("v2 %s = %+v, want counter %v", name, metric, value)
			}
		}
	}
	for name := range want {
		if !seen[name] {
			t.Errorf("v2 does not expose %s", name)
		}
	}
	groups := newMetricGroups(prometheus.NewRegistry())
	families, err := groups.mgGatherers[replicationCollectorPath].Gather()
	if err != nil {
		t.Fatal(err)
	}
	seen = make(map[string]bool)
	for _, family := range families {
		for name, value := range want {
			if family.GetName() != replicationCollectorPath.metricPrefix()+"_"+name {
				continue
			}
			seen[name] = true
			if len(family.Metric) != 1 || family.Metric[0].Counter == nil || family.Metric[0].GetCounter().GetValue() != value {
				t.Errorf("v3 %s = %v, want counter %v", name, family, value)
			}
		}
	}
	for name := range want {
		if !seen[name] {
			t.Errorf("v3 registry does not expose %s", name)
		}
	}
}

type replicationMRFTestObjectLayer struct{ ObjectLayer }

func TestReplicationObjectDeleteWorkerAffinity(t *testing.T) {
	p := &ReplicationPool{ctx: t.Context(), workers: make([]chan ReplicationWorkerOperation, 8)}
	for i := range p.workers {
		p.workers[i] = make(chan ReplicationWorkerOperation, 2)
	}
	for _, op := range []replication.Type{replication.ObjectReplicationType, replication.HealReplicationType, replication.ExistingObjectReplicationType} {
		for i := range 10 {
			name := fmt.Sprintf("object-%d", i)
			p.queueReplicaTask(ReplicateObjectInfo{Bucket: "bucket", Name: name, OpType: op})
			p.queueReplicaDeleteTask(DeletedObjectReplicationInfo{Bucket: "bucket", DeletedObject: DeletedObject{ObjectName: name}})
			objectWorker, deleteWorker := -1, -1
			for idx, ch := range p.workers {
				for len(ch) > 0 {
					switch (<-ch).(type) {
					case ReplicateObjectInfo:
						objectWorker = idx
					case DeletedObjectReplicationInfo:
						deleteWorker = idx
					}
				}
			}
			if objectWorker < 0 || objectWorker != deleteWorker {
				t.Fatalf("operation %d, %s: object worker %d != delete worker %d", op, name, objectWorker, deleteWorker)
			}
		}
	}
}
