package server

import (
	"sync"
)

type CapacityBlockReason string

const (
	CapacityBlockReasonNone   CapacityBlockReason = "none"
	CapacityBlockReasonMemory CapacityBlockReason = "memory"
	CapacityBlockReasonVCPU   CapacityBlockReason = "vcpu"
	CapacityBlockReasonBoth   CapacityBlockReason = "both"
)

type CapacitySnapshot struct {
	MemoryLimitMib    int64
	MemoryReservedMib int64
	VCPULimit         int64
	VCPUReserved      int64
	Outstanding       int
}

type capacityReservationState struct {
	memoryMib int64
	vcpuCount int64
	released  bool
}

type CapacityReservation struct {
	manager   *CapacityManager
	id        uint64
	memoryMib int64
	vcpuCount int64
	release   sync.Once
}

func (r *CapacityReservation) Release() {
	if r == nil || r.manager == nil {
		return
	}

	r.release.Do(func() {
		r.manager.release(r.id)
	})
}

type CapacityManager struct {
	mu                sync.Mutex
	nextReservationID uint64
	memoryLimitMib    int64
	memoryReservedMib int64
	vcpuLimit         int64
	vcpuReserved      int64
	reservations      map[uint64]*capacityReservationState
}

func NewCapacityManager(cfg *CapacityConfig) *CapacityManager {
	manager := &CapacityManager{
		reservations: make(map[uint64]*capacityReservationState),
	}
	if cfg != nil {
		manager.memoryLimitMib = cfg.MemoryLimitMib
		manager.vcpuLimit = cfg.VCPULimit
	}

	metricCapacityMemoryLimit.Set(float64(manager.memoryLimitMib))
	metricCapacityMemoryReserved.Set(0)
	metricCapacityVCPULimit.Set(float64(manager.vcpuLimit))
	metricCapacityVCPUReserved.Set(0)

	return manager
}

func (m *CapacityManager) Enabled() bool {
	if m == nil {
		return false
	}

	return m.memoryLimitMib > 0 || m.vcpuLimit > 0
}

func (m *CapacityManager) ReserveUpTo(count int, memoryMib, vcpuCount int64) ([]*CapacityReservation, CapacityBlockReason) {
	if m == nil || count <= 0 {
		return nil, CapacityBlockReasonNone
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	allowed := count
	if m.memoryLimitMib > 0 {
		memorySlots := int((m.memoryLimitMib - m.memoryReservedMib) / memoryMib)
		if memorySlots < 0 {
			memorySlots = 0
		}
		if memorySlots < allowed {
			allowed = memorySlots
		}
	}

	if m.vcpuLimit > 0 {
		vcpuSlots := int((m.vcpuLimit - m.vcpuReserved) / vcpuCount)
		if vcpuSlots < 0 {
			vcpuSlots = 0
		}
		if vcpuSlots < allowed {
			allowed = vcpuSlots
		}
	}

	reservations := make([]*CapacityReservation, 0, allowed)
	for i := 0; i < allowed; i++ {
		m.nextReservationID++
		id := m.nextReservationID
		m.reservations[id] = &capacityReservationState{
			memoryMib: memoryMib,
			vcpuCount: vcpuCount,
		}
		m.memoryReservedMib += memoryMib
		m.vcpuReserved += vcpuCount
		reservations = append(reservations, &CapacityReservation{
			manager:   m,
			id:        id,
			memoryMib: memoryMib,
			vcpuCount: vcpuCount,
		})
	}

	metricCapacityMemoryReserved.Set(float64(m.memoryReservedMib))
	metricCapacityVCPUReserved.Set(float64(m.vcpuReserved))

	if allowed == count {
		return reservations, CapacityBlockReasonNone
	}

	memoryBlocked := m.memoryLimitMib > 0 && m.memoryReservedMib+memoryMib > m.memoryLimitMib
	vcpuBlocked := m.vcpuLimit > 0 && m.vcpuReserved+vcpuCount > m.vcpuLimit

	switch {
	case memoryBlocked && vcpuBlocked:
		return reservations, CapacityBlockReasonBoth
	case memoryBlocked:
		return reservations, CapacityBlockReasonMemory
	case vcpuBlocked:
		return reservations, CapacityBlockReasonVCPU
	default:
		return reservations, CapacityBlockReasonNone
	}
}

func (m *CapacityManager) Snapshot() CapacitySnapshot {
	if m == nil {
		return CapacitySnapshot{}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	return CapacitySnapshot{
		MemoryLimitMib:    m.memoryLimitMib,
		MemoryReservedMib: m.memoryReservedMib,
		VCPULimit:         m.vcpuLimit,
		VCPUReserved:      m.vcpuReserved,
		Outstanding:       len(m.reservations),
	}
}

func (m *CapacityManager) release(id uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	reservation, ok := m.reservations[id]
	if !ok || reservation.released {
		return
	}

	reservation.released = true
	m.memoryReservedMib -= reservation.memoryMib
	m.vcpuReserved -= reservation.vcpuCount
	delete(m.reservations, id)

	metricCapacityMemoryReserved.Set(float64(m.memoryReservedMib))
	metricCapacityVCPUReserved.Set(float64(m.vcpuReserved))
}
