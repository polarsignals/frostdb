package frostdb

import (
	"sync/atomic"
)

type Txn struct {
	TxnID       uint64
	TxnMetadata []byte
}

type atomicTxn struct {
	atomic.Pointer[Txn]
}

func (t *atomicTxn) Load() Txn {
	txn := t.Pointer.Load()
	if txn == nil {
		return Txn{}
	}
	return *txn
}

func (t *atomicTxn) Store(txn Txn) {
	t.Pointer.Store(&txn)
}
