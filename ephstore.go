package gotimeout

import (
	"time"
)

// EphemeralStoreValue represents any value which may be stored in an EphemeralKeyValueStore.
type EphemeralStoreValue interface{}

// EphemeralKeyValueStore provides an expiring key-value store whose contents are not saved to disk.
// It acts in a manner not dissimilar from a map.
type EphemeralKeyValueStore struct {
	expirator *Expirator
	values    map[string]EphemeralStoreValue
}

type ephemeralExpirationProxy ExpirableID

func (e ephemeralExpirationProxy) ExpirationID() ExpirableID {
	return ExpirableID(e)
}

// NewEphemeralKeyValueStore returns a new EphemeralKeyValueStore whose expiration daemon is already running.
func NewEphemeralKeyValueStore() *EphemeralKeyValueStore {
	v := &EphemeralKeyValueStore{
		values: make(map[string]EphemeralStoreValue),
	}

	v.expirator = NewExpirator("", v)
	return v
}

// Put places the given object into the key-value store and queues its expiration.
func (e *EphemeralKeyValueStore) Put(k string, v EphemeralStoreValue, lifespan time.Duration) {
	e.values[k] = v
	e.expirator.ExpireObject(ephemeralExpirationProxy(k), lifespan)
}

// Get returns the object referred to by the given key.
func (e *EphemeralKeyValueStore) Get(k string) (v EphemeralStoreValue, ok bool) {
	v, ok = e.values[k]
	return
}

// Delete removes the object referred to by the given key from the key-value store and stays its execution.
func (e *EphemeralKeyValueStore) Delete(k string) {
	e.expirator.CancelObjectExpiration(ephemeralExpirationProxy(k))
	delete(e.values, k)
}

func (e *EphemeralKeyValueStore) GetExpirable(id ExpirableID) (Expirable, error) {
	_, ok := e.values[string(id)]
	if !ok {
		return nil, nil
	}
	return ephemeralExpirationProxy(id), nil
}

func (e *EphemeralKeyValueStore) DestroyExpirable(ex Expirable) {
	delete(e.values, string(ex.ExpirationID()))
}
