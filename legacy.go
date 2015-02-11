package gotimeout

import (
	"encoding/gob"
	"os"
	"time"
)

type legacyUpgradingGobFileAdapter struct {
	*GobFileAdapter
}

func (a *legacyUpgradingGobFileAdapter) LoadExpirationHandles() (*HandleMap, error) {
	hm, err := a.GobFileAdapter.LoadExpirationHandles()
	if err == nil {
		return hm, err
	}

	return a.attemptUpgrade()
}

func (a *legacyUpgradingGobFileAdapter) attemptUpgrade() (*HandleMap, error) {
	file, err := os.Open(a.GobFileAdapter.filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var oldMap map[ExpirableID]struct {
		ExpirationTime time.Time
		ID             ExpirableID
	}
	dec := gob.NewDecoder(file)
	err = dec.Decode(&oldMap)
	if err != nil {
		return nil, err
	}

	hm := newHandleMap()
	for k, v := range oldMap {
		hm.m[k] = &Handle{v.ExpirationTime, v.ID, nil}
	}

	return hm, nil
}
