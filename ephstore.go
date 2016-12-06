package gotimeout

import (
	"sync"
	"time"
)

// MapValue represents any value which may be stored in an Map.
type MapValue interface{}

// Map provides an expiring key-value store whose contents are not saved to disk.
// It acts in a manner not dissimilar from a map.
type Map struct {
	expirator *Expirator
	values    map[string]MapValue

	mu   sync.RWMutex
	once sync.Once
}

type expirationProxy ExpirableID

func (e expirationProxy) ExpirationID() ExpirableID {
	return ExpirableID(e)
}

// NewMap returns a new Map whose expiration daemon is already running.
func NewMap() *Map {
	v := &Map{
		values: make(map[string]MapValue),
	}

	v.expirator = NewExpirator("", v)
	return v
}

func (e *Map) init() {
	e.once.Do(func() {
		e.mu.Lock()

		if e.values == nil {
			e.values = make(map[string]MapValue)
		}

		if e.expirator == nil {
			e.expirator = NewExpirator("", e)
		}

		e.mu.Unlock()
	})
}

// Put places the given object into the key-value store and queues its expiration.
func (e *Map) Put(k string, v MapValue, lifespan time.Duration) {
	e.init()

	e.mu.Lock()
	defer e.mu.Unlock()

	e.values[k] = v
	e.expirator.ExpireObject(expirationProxy(k), lifespan)
}

// Get returns the object referred to by the given key.
func (e *Map) Get(k string) (v MapValue, ok bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.values == nil {
		return nil, false
	}

	v, ok = e.values[k]
	return
}

// Delete removes the object referred to by the given key from the key-value store and stays its execution.
func (e *Map) Delete(k string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.values == nil {
		return
	}

	e.expirator.CancelObjectExpiration(expirationProxy(k))
	delete(e.values, k)
}

func (e *Map) GetExpirable(id ExpirableID) Expirable {
	e.mu.RLock()
	defer e.mu.RUnlock()

	_, ok := e.values[string(id)]
	if !ok {
		return nil
	}
	return expirationProxy(id)
}

func (e *Map) DestroyExpirable(ex Expirable) {
	e.mu.Lock()
	defer e.mu.Unlock()

	delete(e.values, string(ex.ExpirationID()))
}
