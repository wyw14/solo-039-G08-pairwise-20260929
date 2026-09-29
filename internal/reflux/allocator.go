package reflux

import (
	"errors"
	"github.com/wyw14/solo-039/internal/model"
	"sort"
	"sync"
)

type Request struct {
	Owner    string
	Capacity float64
	Fence    uint64
	Priority int
}

// ErrDeferred is returned (wrapping the coordinator's CapacityError) when the
// head request could not be granted because healthy capacity is insufficient.
// The request remains queued at the front and can be retried with Next once
// pump health frees capacity; it is never discarded or over-allocated.
var ErrDeferred = errors.New("reflux request deferred: capacity unavailable")

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

// Next grants the highest-priority pending request. If healthy capacity cannot
// cover it, the request is put back at the head of the queue and an error
// joining ErrDeferred with the detailed CapacityError is returned.
func (a *Allocator) Next() (model.Lease, error) {
	a.mu.Lock()
	if len(a.queue) == 0 {
		a.mu.Unlock()
		return model.Lease{}, errors.New("reflux queue empty")
	}
	request := a.queue[0]
	a.queue = a.queue[1:]
	a.mu.Unlock()

	lease, err := a.coordinator.Acquire(request.Owner, request.Capacity, request.Fence)
	if err != nil {
		a.mu.Lock()
		a.queue = append([]Request{request}, a.queue...)
		a.mu.Unlock()
		return model.Lease{}, errors.Join(ErrDeferred, err)
	}
	return lease, nil
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
