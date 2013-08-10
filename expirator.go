// Package gotimeout provides a facility for expiring objects after a set period of time.
// It is meant to be used in a manner not dissimilar from a daemon - the key difference
// being that it runs in a goroutine as part of your process.
package gotimeout

import (
	"encoding/gob"
	"os"
	"time"
)

// ExpirableID provides Opaque identification for an Expirable object.
type ExpirableID string

type expirationHandle struct {
	ExpirationTime  time.Time
	ID              ExpirableID
	expirationTimer *time.Timer
}

// Expirator provides the primary mechanism of gotimeout: an expiration daemon.
type Expirator struct {
	store               ExpirableStore
	dataPath            string
	expirationMap       map[ExpirableID]*expirationHandle
	expirationChannel   chan *expirationHandle
	flushRequired       bool
	urgentFlushRequired bool
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
	e := &Expirator{
		store:             store,
		dataPath:          path,
		expirationChannel: make(chan *expirationHandle, 1000),
	}
	go e.run()
	return e
}

func (e *Expirator) canSave() bool {
	return e.dataPath != ""
}

func (e *Expirator) loadExpirations() {
	if !e.canSave() {
		return
	}

	file, err := os.Open(e.dataPath)
	if err != nil {
		return
	}

	gobDecoder := gob.NewDecoder(file)
	tempMap := make(map[ExpirableID]*expirationHandle)
	gobDecoder.Decode(&tempMap)
	file.Close()

	for _, v := range tempMap {
		e.registerExpirationHandle(v)
	}
}

func (e *Expirator) saveExpirations() {
	if !e.canSave() {
		return
	}

	if e.expirationMap == nil {
		return
	}

	file, err := os.Create(e.dataPath)
	if err != nil {
		return
	}

	gobEncoder := gob.NewEncoder(file)
	gobEncoder.Encode(e.expirationMap)

	file.Close()

	e.flushRequired, e.urgentFlushRequired = false, false
}

func (e *Expirator) registerExpirationHandle(ex *expirationHandle) {
	expiryFunc := func() { e.expirationChannel <- ex }

	if e.expirationMap == nil {
		e.expirationMap = make(map[ExpirableID]*expirationHandle)
	}

	if ex.expirationTimer != nil {
		e.cancelExpirationHandle(ex)
	}

	now := time.Now()
	if ex.ExpirationTime.After(now) {
		e.expirationMap[ex.ID] = ex
		e.urgentFlushRequired = true

		ex.expirationTimer = time.AfterFunc(ex.ExpirationTime.Sub(now), expiryFunc)
	} else {
		expiryFunc()
	}
}

func (e *Expirator) cancelExpirationHandle(ex *expirationHandle) {
	ex.expirationTimer.Stop()
	delete(e.expirationMap, ex.ID)
	e.urgentFlushRequired = true

}

func (e *Expirator) run() {
	go e.loadExpirations()
	var flushTickerChan, urgentFlushTickerChan <-chan time.Time
	if e.canSave() {
		flushTickerChan, urgentFlushTickerChan = time.NewTicker(30*time.Second).C, time.NewTicker(1*time.Second).C
	}
	for {
		select {
		// 30-second flush timer (only save if changed)
		case _ = <-flushTickerChan:
			if e.expirationMap != nil && (e.flushRequired || e.urgentFlushRequired) {
				e.saveExpirations()
			}
		// 1-second flush timer (only save if *super-urgent, but still throttle)
		case _ = <-urgentFlushTickerChan:
			if e.expirationMap != nil && e.urgentFlushRequired {
				e.saveExpirations()
			}
		case expiration := <-e.expirationChannel:
			delete(e.expirationMap, expiration.ID)

			if expirable := e.store.GetExpirable(expiration.ID); expirable != nil {
				e.store.DestroyExpirable(expirable)
			}

			e.flushRequired = true
		}
	}
}

// ExpireObject registers an object for expiration after a given duration.
func (e *Expirator) ExpireObject(ex Expirable, dur time.Duration) {
	id := ex.ExpirationID()
	exh, ok := e.expirationMap[id]
	if !ok {
		exh = &expirationHandle{ID: id}
	}
	exh.ExpirationTime = time.Now().Add(dur)
	e.registerExpirationHandle(exh)
}

// CancelObjectExpiration stays the execution of an object, or does nothing if the given object was not going to be executed.
func (e *Expirator) CancelObjectExpiration(ex Expirable) {
	id := ex.ExpirationID()
	exh, ok := e.expirationMap[id]
	if ok {
		e.cancelExpirationHandle(exh)
	}
}

// ObjectHasExpiration returns whether the given object has a registered expiration.
func (e *Expirator) ObjectHasExpiration(ex Expirable) bool {
	id := ex.ExpirationID()
	_, ok := e.expirationMap[id]
	return ok
}
