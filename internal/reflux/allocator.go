package reflux

import (
	"errors"
	"github.com/wyw14/solo-039/internal/model"
	"sort"
	"sync"
)

type Request struct {
	Owner    string
	Pump     string
	Capacity float64
	Fence    uint64
	Priority int
}

type Allocator struct {
	mu          sync.Mutex
	coordinator *Coordinator
	queue       []Request
}

func NewAllocator(coordinator *Coordinator) *Allocator {
	return &Allocator{coordinator: coordinator, queue: make([]Request, 0, 16)}
}

func (a *Allocator) Submit(request Request) error {
	if request.Owner == "" || request.Capacity <= 0 {
		return errors.New("invalid reflux request")
	}
	a.mu.Lock()
	a.queue = append(a.queue, request)
	sort.SliceStable(a.queue, func(i, j int) bool { return a.queue[i].Priority > a.queue[j].Priority })
	a.mu.Unlock()
	return nil
}

func (a *Allocator) Next() (model.Lease, error) {
	a.mu.Lock()
	if len(a.queue) == 0 {
		a.mu.Unlock()
		return model.Lease{}, errors.New("reflux queue empty")
	}
	request := a.queue[0]
	a.queue = a.queue[1:]
	a.mu.Unlock()
	return a.coordinator.AcquireOn(request.Pump, request.Owner, request.Capacity, request.Fence)
}

func (a *Allocator) Pending() []Request {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]Request(nil), a.queue...)
}

func (a *Allocator) Cancel(owner string) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	kept := a.queue[:0]
	removed := 0
	for _, request := range a.queue {
		if request.Owner == owner {
			removed++
			continue
		}
		kept = append(kept, request)
	}
	a.queue = kept
	return removed
}
