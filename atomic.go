package sqlite

import "sync/atomic"

// noCopy may be embedded into structs which must not be copied
// after the first use.
//
// See https://github.com/golang/go/issues/8005#issuecomment-190753527
// for details.
type noCopy struct{}

// Lock is a no-op used by -copylocks checker from `go vet`.
func (*noCopy) Lock() {}

type atomicBool struct {
	noCopy noCopy
	value  int32
}

func (b *atomicBool) Set(value bool) {
	if value {
		atomic.StoreInt32(&b.value, 1)
	} else {
		atomic.StoreInt32(&b.value, 0)
	}
}

func (b *atomicBool) IsSet() bool {
	return atomic.LoadInt32(&b.value) > 0
}
