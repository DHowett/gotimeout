// Package gotimeout provides a facility for expiring objects after a set period of time.
// It is meant to be used in a manner not dissimilar from a daemon - the key difference
// being that it runs in a goroutine as part of your process.
package gotimeout

import (
	"bytes"
	"encoding/gob"
	"sync"
	"time"
)

// ExpirableID provides Opaque identification for an Expirable object.
type ExpirableID string

// Handle is an opaque token marking an object for destruction.
type Handle struct {
	expirationTime  time.Time
	id              ExpirableID
	expirationTimer *time.Timer
}

func (h *Handle) MarshalBinary() ([]byte, error) {
	b := &bytes.Buffer{}
	enc := gob.NewEncoder(b)
	enc.Encode(string(h.id))
	enc.Encode(h.expirationTime)

	return b.Bytes(), nil
}

func (h *Handle) UnmarshalBinary(b []byte) error {
	r := bytes.NewReader(b)
	dec := gob.NewDecoder(r)

	var s string
	dec.Decode(&s)
	h.id = ExpirableID(s)
	dec.Decode(&h.expirationTime)
	return nil
}

// HandleMap is an opaque storage structure for expiring objects.
type HandleMap struct {
	m map[ExpirableID]*Handle
}

func newHandleMap() *HandleMap {
	return &HandleMap{
		m: make(map[ExpirableID]*Handle),
	}
}

func (h *HandleMap) MarshalBinary() ([]byte, error) {
	b := &bytes.Buffer{}
	enc := gob.NewEncoder(b)
	enc.Encode(h.m)
	return b.Bytes(), nil
}

func (h *HandleMap) UnmarshalBinary(b []byte) error {
	r := bytes.NewReader(b)
	dec := gob.NewDecoder(r)
	dec.Decode(&h.m)
	return nil
}

// Expirator provides the primary mechanism of gotimeout: an expiration daemon.
type Expirator struct {
	// A channel upon which a client can receive error messages from the daemon.
	ErrorChannel <-chan error

	adapter             StorageAdapter
	store               ExpirableStore
	expirationMap       *HandleMap
	expirationChannel   chan *Handle
	flushRequired       bool
	urgentFlushRequired bool
	errorChannelSend    chan<- error
	wg                  sync.WaitGroup
	mu                  sync.RWMutex
}

// Expirable represents any object that might be expired.
type Expirable interface {
	ExpirationID() ExpirableID
}

type ExpirableStore interface {
	GetExpirable(ExpirableID) Expirable
	DestroyExpirable(Expirable)
}

// NewExpirator returns a new Expirator given a store and location to which to serialize expiration metadata.
// path may be "", denoting that this Expirator should not save anything. The returned expiration daemon will have
// already been started.
func NewExpirator(path string, store ExpirableStore) *Expirator {
	var sa StorageAdapter
	if path == "" {
		sa = NoopAdapter{}
	} else {
		sa = &legacyUpgradingGobFileAdapter{NewGobFileAdapter(path)}
	}
	return NewExpiratorWithStorage(sa, store)
}

// NewExpiratorWithStorage returns a new Expirator given a store and means with which to store expiration metadata.
// The returned expiration daemon will have already been started.
func NewExpiratorWithStorage(sa StorageAdapter, store ExpirableStore) *Expirator {
	e := &Expirator{
		adapter:           sa,
		store:             store,
		expirationChannel: make(chan *Handle, 1000),
		expirationMap:     newHandleMap(),
	}

	errorChannel := make(chan error, 1)
	e.ErrorChannel = (<-chan error)(errorChannel)
	e.errorChannelSend = (chan<- error)(errorChannel)

	go e.run()
	return e
}

func (e *Expirator) loadExpirations() error {
	hm, err := e.adapter.LoadExpirationHandles()
	if err != nil {
		return err
	}

	if hm == nil {
		return nil
	}

	for _, v := range hm.m {
		e.registerExpirationHandle(v)
	}

	return nil
}

func (e *Expirator) saveExpirations() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.expirationMap == nil {
		return nil
	}
	err := e.adapter.SaveExpirationHandles(e.expirationMap)
	if err != nil {
		return err
	}

	e.flushRequired, e.urgentFlushRequired = false, false

	return nil
}

func (e *Expirator) registerExpirationHandle(ex *Handle) {
	e.mu.Lock()
	defer e.mu.Unlock()

	expiryFunc := func() { e.expirationChannel <- ex }

	if ex.expirationTimer != nil {
		e.cancelExpirationHandle(ex)
	}

	now := time.Now()
	if ex.expirationTime.After(now) {
		e.expirationMap.m[ex.id] = ex
		e.urgentFlushRequired = true

		ex.expirationTimer = time.AfterFunc(ex.expirationTime.Sub(now), expiryFunc)
	} else {
		expiryFunc()
	}
}

func (e *Expirator) cancelExpirationHandle(ex *Handle) {
	ex.expirationTimer.Stop()
	delete(e.expirationMap.m, ex.id)
	e.urgentFlushRequired = true

}

func (e *Expirator) run() {
	e.wg.Add(1)
	go func() {
		if err := e.loadExpirations(); err != nil {
			e.errorChannelSend <- err
		}
		e.wg.Done()
	}()
	var flushTickerChan, urgentFlushTickerChan <-chan time.Time
	if e.adapter.RequiresFlush() {
		flushTickerChan, urgentFlushTickerChan = time.NewTicker(30*time.Second).C, time.NewTicker(1*time.Second).C
	}
	e.wg.Wait()
	for {
		select {
		// 30-second flush timer (only save if changed)
		case _ = <-flushTickerChan:
			if e.expirationMap != nil && (e.flushRequired || e.urgentFlushRequired) {
				if err := e.saveExpirations(); err != nil {
					e.errorChannelSend <- err
				}
			}
		// 1-second flush timer (only save if *super-urgent, but still throttle)
		case _ = <-urgentFlushTickerChan:
			if e.expirationMap != nil && e.urgentFlushRequired {
				if err := e.saveExpirations(); err != nil {
					e.errorChannelSend <- err
				}
			}
		case expiration := <-e.expirationChannel:
			e.mu.Lock()

			delete(e.expirationMap.m, expiration.id)
			e.flushRequired = true

			e.mu.Unlock()

			if expirable := e.store.GetExpirable(expiration.id); expirable != nil {
				e.store.DestroyExpirable(expirable)
			}
		}
	}
}

// ExpireObject registers an object for expiration after a given duration.
func (e *Expirator) ExpireObject(ex Expirable, dur time.Duration) {
	e.mu.RLock()
	id := ex.ExpirationID()
	exh, ok := e.expirationMap.m[id]
	if !ok {
		exh = &Handle{id: id}
	}
	exh.expirationTime = time.Now().Add(dur)
	e.mu.RUnlock()
	e.registerExpirationHandle(exh)
}

// CancelObjectExpiration stays the execution of an object, or does nothing if the given object was not going to be executed.
func (e *Expirator) CancelObjectExpiration(ex Expirable) {
	e.mu.Lock()
	defer e.mu.Unlock()
	id := ex.ExpirationID()
	exh, ok := e.expirationMap.m[id]
	if ok {
		e.cancelExpirationHandle(exh)
	}
}

// ObjectHasExpiration returns whether the given object has a registered expiration.
func (e *Expirator) ObjectHasExpiration(ex Expirable) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	id := ex.ExpirationID()
	_, ok := e.expirationMap.m[id]
	return ok
}

// Len returns the number of objects registered for expiration.
func (e *Expirator) Len() (l int) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	l = 0
	if e.expirationMap != nil {
		l = len(e.expirationMap.m)
	}
	return
}
