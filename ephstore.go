package gotimeout

import (
	"time"
)

// MapValue represents any value which may be stored in an Map.
type MapValue interface{}

// Map provides an expiring key-value store whose contents are not saved to disk.
// It acts in a manner not dissimilar from a map.
type Map struct {
	expirator *Expirator
	values    map[string]MapValue
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

// Put places the given object into the key-value store and queues its expiration.
func (e *Map) Put(k string, v MapValue, lifespan time.Duration) {
	e.values[k] = v
	e.expirator.ExpireObject(expirationProxy(k), lifespan)
}

// Get returns the object referred to by the given key.
func (e *Map) Get(k string) (v MapValue, ok bool) {
	v, ok = e.values[k]
	return
}

// Delete removes the object referred to by the given key from the key-value store and stays its execution.
func (e *Map) Delete(k string) {
	e.expirator.CancelObjectExpiration(expirationProxy(k))
	delete(e.values, k)
}

func (e *Map) GetExpirable(id ExpirableID) Expirable {
	_, ok := e.values[string(id)]
	if !ok {
		return nil
	}
	return expirationProxy(id)
}

func (e *Map) DestroyExpirable(ex Expirable) {
	delete(e.values, string(ex.ExpirationID()))
}
