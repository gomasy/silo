// Copyright (c) 2026 Ruohang Feng
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/cachevalue"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	cpustats "github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/load"
)

// These tests replace global resource metrics and must not run in parallel.
// Concurrent writers must finish before the fixture is restored.
func setCPUResourceMetricsForTest(t *testing.T, value map[MetricSubsystem]ResourceMetrics) {
	t.Helper()
	resourceMetricsMapMu.Lock()
	saved := resourceMetricsMap
	resourceMetricsMap = value
	resourceMetricsMapMu.Unlock()
	t.Cleanup(func() {
		resourceMetricsMapMu.Lock()
		resourceMetricsMap = saved
		resourceMetricsMapMu.Unlock()
	})
}

func newCPUMetricsTestRegistry() *prometheus.Registry {
	c := &metricsCache{cpuMetrics: cachevalue.NewFromFunc(time.Hour, cachevalue.Opts{},
		func(context.Context) (madmin.CPUMetrics, error) {
			return madmin.CPUMetrics{
				CPUCount: 4,
				LoadStat: &load.AvgStat{Load1: 2},
				TimesStat: &cpustats.TimesStat{
					User: 10, System: 20, Idle: 60, Iowait: 5, Nice: 3, Steal: 2,
				},
			}, nil
		})}
	_, _ = c.cpuMetrics.Get() // Warm the cache: the cache does not protect the resource map.
	g := NewMetricsGroup(systemCPUCollectorPath, []MetricDescriptor{
		sysCPUAvgIdleMD, sysCPUAvgIOWaitMD, sysCPULoadMD, sysCPULoadPercMD,
		sysCPUNiceMD, sysCPUStealMD, sysCPUSystemMD, sysCPUUserMD,
	}, loadCPUMetrics)
	g.SetCache(c)
	r := prometheus.NewPedanticRegistry()
	r.MustRegister(g)
	return r
}

func TestLoadCPUMetricsValues(t *testing.T) {
	for _, tc := range []struct {
		name string
		data map[MetricSubsystem]ResourceMetrics
		want map[string]float64
	}{
		{name: "nil-map"},
		{name: "empty-map", data: map[MetricSubsystem]ResourceMetrics{}},
		{name: "no-cpu", data: map[MetricSubsystem]ResourceMetrics{memSubsystem: {}}},
		{name: "nil-cpu", data: map[MetricSubsystem]ResourceMetrics{cpuSubsystem: nil}},
		{name: "empty-cpu", data: map[MetricSubsystem]ResourceMetrics{cpuSubsystem: {}}},
		{name: "idle-only", data: map[MetricSubsystem]ResourceMetrics{cpuSubsystem: {
			getResourceKey(cpuIdle, nil): {Avg: 87.654},
		}}, want: map[string]float64{"minio_system_cpu_avg_idle": 87.65}},
		{name: "iowait-only", data: map[MetricSubsystem]ResourceMetrics{cpuSubsystem: {
			getResourceKey(cpuIOWait, nil): {Avg: 1.236},
		}}, want: map[string]float64{"minio_system_cpu_avg_iowait": 1.24}},
		{name: "both-rounded", data: map[MetricSubsystem]ResourceMetrics{cpuSubsystem: {
			getResourceKey(cpuIdle, nil):   {Avg: 87.654},
			getResourceKey(cpuIOWait, nil): {Avg: 1.236},
		}}, want: map[string]float64{"minio_system_cpu_avg_idle": 87.65, "minio_system_cpu_avg_iowait": 1.24}},
		{name: "zero-keeps-existing-omission", data: map[MetricSubsystem]ResourceMetrics{cpuSubsystem: {
			getResourceKey(cpuIdle, nil):   {Avg: 0},
			getResourceKey(cpuIOWait, nil): {Avg: 0},
		}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setCPUResourceMetricsForTest(t, tc.data)
			families, err := newCPUMetricsTestRegistry().Gather()
			if err != nil {
				t.Fatal(err)
			}
			want := map[string]float64{
				"minio_system_cpu_load": 2, "minio_system_cpu_load_perc": 50,
				"minio_system_cpu_nice": 3, "minio_system_cpu_steal": 2,
				"minio_system_cpu_system": 20, "minio_system_cpu_user": 10,
			}
			for k, v := range tc.want {
				want[k] = v
			}
			if len(families) != len(want) {
				t.Fatalf("got %d families, want %d", len(families), len(want))
			}
			for _, family := range families {
				v, ok := want[family.GetName()]
				if !ok || family.GetType() != dto.MetricType_GAUGE || len(family.Metric) != 1 || family.Metric[0].GetGauge().GetValue() != v {
					t.Errorf("unexpected family: %v (want %v)", family, want)
				}
			}
		})
	}
}

func TestLoadCPUMetricsConcurrentUpdate(t *testing.T) {
	for _, subsystem := range []MetricSubsystem{cpuSubsystem, memSubsystem} {
		for _, readers := range []int{1, 4} {
			t.Run(fmt.Sprintf("writer-%s/readers-%d", subsystem, readers), func(t *testing.T) {
				setCPUResourceMetricsForTest(t, map[MetricSubsystem]ResourceMetrics{})
				updateResourceMetrics(cpuSubsystem, cpuIdle, 80, nil, false)
				updateResourceMetrics(cpuSubsystem, cpuIOWait, 5, nil, false)
				r := newCPUMetricsTestRegistry()
				stop := make(chan struct{})
				done := make(chan struct{})
				ready := make(chan struct{})
				var updates atomic.Uint64
				go func() {
					defer close(done)
					if subsystem == cpuSubsystem {
						updateResourceMetrics(subsystem, cpuIdle, 80, nil, false)
					} else {
						updateResourceMetrics(subsystem, memUsed, 1024, nil, false)
					}
					close(ready)
					for {
						select {
						case <-stop:
							return
						default:
						}
						if subsystem == cpuSubsystem {
							updateResourceMetrics(subsystem, cpuIdle, 80, nil, false)
							updateResourceMetrics(subsystem, cpuIOWait, 5, nil, false)
						} else {
							updateResourceMetrics(subsystem, memUsed, 1024, nil, false)
						}
						updates.Add(1)
						runtime.Gosched()
					}
				}()
				<-ready
				defer func() { close(stop); <-done }()
				var wg sync.WaitGroup
				for range readers {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for range 1000 {
							families, err := r.Gather()
							if err != nil || len(families) != 8 {
								t.Errorf("Gather: families=%d, error=%v", len(families), err)
								return
							}
						}
					}()
				}
				wg.Wait()
				if updates.Load() == 0 {
					t.Fatal("writer made no progress")
				}
				t.Logf("completed %d gathers with %d concurrent update iterations", readers*1000, updates.Load())
			})
		}
	}
}
