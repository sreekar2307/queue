package storage

import bolt "go.etcd.io/bbolt"

type Transaction interface {
	Commit() error
	Rollback() error
}

type BoltDbTransactionWrapper struct {
	BoltTx *bolt.Tx
}

func (b *BoltDbTransactionWrapper) Commit() error {
	if b.BoltTx.Writable() {
		return b.BoltTx.Commit()
	}
	return nil
}

func (b *BoltDbTransactionWrapper) Rollback() error {
	return b.BoltTx.Rollback()
}
