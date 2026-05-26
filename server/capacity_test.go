package server

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCapacityManagerReserveAndRelease(t *testing.T) {
	manager := NewCapacityManager(&CapacityConfig{
		MemoryLimitMib: 4096,
		VCPULimit:      4,
	})

	reservations, reason := manager.ReserveUpTo(3, 1024, 1)
	require.Equal(t, CapacityBlockReasonNone, reason)
	require.Len(t, reservations, 3)

	snapshot := manager.Snapshot()
	assert.Equal(t, int64(3072), snapshot.MemoryReservedMib)
	assert.Equal(t, int64(3), snapshot.VCPUReserved)
	assert.Equal(t, 3, snapshot.Outstanding)

	reservations[0].Release()
	reservations[0].Release()

	snapshot = manager.Snapshot()
	assert.Equal(t, int64(2048), snapshot.MemoryReservedMib)
	assert.Equal(t, int64(2), snapshot.VCPUReserved)
	assert.Equal(t, 2, snapshot.Outstanding)

	reservations[1].Release()
	reservations[2].Release()

	snapshot = manager.Snapshot()
	assert.Equal(t, int64(0), snapshot.MemoryReservedMib)
	assert.Equal(t, int64(0), snapshot.VCPUReserved)
	assert.Equal(t, 0, snapshot.Outstanding)
}

func TestCapacityManagerPartialAdmissionAndReason(t *testing.T) {
	manager := NewCapacityManager(&CapacityConfig{
		MemoryLimitMib: 4096,
		VCPULimit:      8,
	})

	reservations, reason := manager.ReserveUpTo(3, 2048, 1)
	require.Equal(t, CapacityBlockReasonMemory, reason)
	require.Len(t, reservations, 2)

	for _, reservation := range reservations {
		reservation.Release()
	}

	manager = NewCapacityManager(&CapacityConfig{
		MemoryLimitMib: 8192,
		VCPULimit:      2,
	})

	reservations, reason = manager.ReserveUpTo(3, 1024, 1)
	require.Equal(t, CapacityBlockReasonVCPU, reason)
	require.Len(t, reservations, 2)

	for _, reservation := range reservations {
		reservation.Release()
	}

	manager = NewCapacityManager(&CapacityConfig{
		MemoryLimitMib: 2048,
		VCPULimit:      2,
	})

	reservations, reason = manager.ReserveUpTo(2, 2048, 2)
	require.Equal(t, CapacityBlockReasonBoth, reason)
	require.Len(t, reservations, 1)
}

func TestCapacityManagerDisabledDimensions(t *testing.T) {
	manager := NewCapacityManager(&CapacityConfig{})

	reservations, reason := manager.ReserveUpTo(5, 2048, 4)
	require.Equal(t, CapacityBlockReasonNone, reason)
	require.Len(t, reservations, 5)

	snapshot := manager.Snapshot()
	assert.Equal(t, int64(10240), snapshot.MemoryReservedMib)
	assert.Equal(t, int64(20), snapshot.VCPUReserved)
	assert.Equal(t, 5, snapshot.Outstanding)
}

func TestCapacityManagerConcurrentReservationsNeverExceedLimit(t *testing.T) {
	manager := NewCapacityManager(&CapacityConfig{
		MemoryLimitMib: 8192,
		VCPULimit:      8,
	})

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reservations, _ := manager.ReserveUpTo(1, 1024, 1)
			for _, reservation := range reservations {
				reservation.Release()
			}
		}()
	}

	wg.Wait()

	snapshot := manager.Snapshot()
	assert.LessOrEqual(t, snapshot.MemoryReservedMib, int64(8192))
	assert.LessOrEqual(t, snapshot.VCPUReserved, int64(8))
	assert.Equal(t, 0, snapshot.Outstanding)
}
