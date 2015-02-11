package gotimeout

import (
	"encoding/gob"
	"os"
)

// StorageAdapter is the interface through which expiration storage is abstracted.
//
// SaveExpirationHandles stores a set of expiration handles, and LoadExpirationHandles returns them unharmed.
//
// Implementation that care about saving their changes in a timely manner should
// return true from RequiresFlush.
type StorageAdapter interface {
	RequiresFlush() bool
	SaveExpirationHandles(*HandleMap) error
	LoadExpirationHandles() (*HandleMap, error)
}

// GobFileAdapter is a StorageAdapter that saves expiration handles in a file via the encoding/gob package.
type GobFileAdapter struct {
	filename string
}

func (a *GobFileAdapter) RequiresFlush() bool {
	return true
}

func (a *GobFileAdapter) SaveExpirationHandles(hm *HandleMap) error {
	file, err := os.Create(a.filename)
	if err != nil {
		return err
	}
	defer file.Close()
	enc := gob.NewEncoder(file)
	err = enc.Encode(hm)
	if err != nil {
		return err
	}
	return nil
}

func (a *GobFileAdapter) LoadExpirationHandles() (*HandleMap, error) {
	file, err := os.Open(a.filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	dec := gob.NewDecoder(file)
	var hm *HandleMap
	err = dec.Decode(&hm)
	if err != nil {
		return nil, err
	}
	return hm, nil
}

func NewGobFileAdapter(filename string) *GobFileAdapter {
	return &GobFileAdapter{filename: filename}
}

// NoopAdapter is a StorageAdapter that does not save expiration handles.
type NoopAdapter struct{}

func (NoopAdapter) RequiresFlush() bool {
	return false
}

func (NoopAdapter) SaveExpirationHandles(*HandleMap) error {
	return nil
}
func (NoopAdapter) LoadExpirationHandles() (*HandleMap, error) {
	return nil, nil
}
