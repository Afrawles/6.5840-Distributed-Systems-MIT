package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here\
	
	key string
	clientId string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	
	lk.key = l
	lk.clientId = kvtest.RandValue(8)
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		val, ver, err := lk.ck.Get(lk.key)

		switch err {
		case rpc.ErrNoKey:
			replyErr := lk.ck.Put(lk.key, lk.clientId, 0) 
			if replyErr == rpc.OK {
				return
			}
			if replyErr == rpc.ErrMaybe {
				val, _, err := lk.ck.Get(lk.key)
				if err == rpc.OK && val == lk.clientId {
					return
				}
			}
		case rpc.OK:
			if val == "" {
				replyErr := lk.ck.Put(lk.key, lk.clientId, ver) 
				if replyErr == rpc.OK {
					return
				}

				if replyErr == rpc.ErrMaybe {
					val, _, err := lk.ck.Get(lk.key)
					if err == rpc.OK && val == lk.clientId {
						return
					}
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	} 

}

func (lk *Lock) Release() {
	// Your code here
	for {
		val, ver, err := lk.ck.Get(lk.key)
		if err == rpc.OK && val == lk.clientId {
			replyErr := lk.ck.Put(lk.key, "", ver)
			if replyErr == rpc.OK {
				return
			}
			if replyErr == rpc.ErrMaybe {
				val, _, err := lk.ck.Get(lk.key)
				if err == rpc.OK && val == "" {
					return
				}
				if err == rpc.OK && val != lk.clientId {
					return
				}
			}
			if replyErr == rpc.ErrVersion {
				return
			}
		} else {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

}
