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
	return b.BoltTx.Commit()
}

func (b *BoltDbTransactionWrapper) Rollback() error {
	return b.BoltTx.Rollback()
}
