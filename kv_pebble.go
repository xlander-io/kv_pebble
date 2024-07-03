package kv_pebble

import (
	"github.com/cockroachdb/pebble"
	"github.com/xlander-io/kv"
)

var Default_W_OP_TRUE = pebble.Sync

var Default_W_OP_FALSE = pebble.NoSync

type KV_PEBBLE struct {
	pebbledb *pebble.DB
}

func NewDB(db_path string) (kv.KVDB, error) {
	level_db_, err := pebble.Open(db_path, nil)
	if err != nil {
		return nil, err
	}

	return &KV_PEBBLE{pebbledb: level_db_}, nil
}

func (db *KV_PEBBLE) Close() error {
	return db.pebbledb.Close()
}

func (db *KV_PEBBLE) Put(key, value []byte, sync bool) error {
	if sync {
		return db.pebbledb.Set(key, value, Default_W_OP_TRUE)
	} else {
		return db.pebbledb.Set(key, value, Default_W_OP_FALSE)
	}
}

func (db *KV_PEBBLE) Delete(key []byte, sync bool) error {
	if sync {
		return db.pebbledb.Delete(key, Default_W_OP_TRUE)
	} else {
		return db.pebbledb.Delete(key, Default_W_OP_FALSE)
	}
}

func (db *KV_PEBBLE) Get(key []byte) (value []byte, err error) {
	value, closer, err := db.pebbledb.Get(key)

	if nil != closer {
		closer.Close()
	}

	return value, err
}

func (db *KV_PEBBLE) WriteBatch(batch *kv.Batch, sync bool) error {
	pebble_batch := db.pebbledb.NewBatch()
	batch.Loop(func(key, val []byte) {
		if val == nil {
			pebble_batch.Delete(key, nil)
		} else {
			pebble_batch.Set(key, val, nil)
		}
	})

	if sync {
		return db.pebbledb.Apply(pebble_batch, Default_W_OP_TRUE)
	} else {
		return db.pebbledb.Apply(pebble_batch, Default_W_OP_FALSE)
	}
}

func (db *KV_PEBBLE) NewIterator(start []byte, limit []byte) kv.Iterator {
	iter, err := db.pebbledb.NewIter(&pebble.IterOptions{LowerBound: start, UpperBound: limit})
	if nil != err {
		return nil
	}
	return &Iterator{iter: iter}
}

type Iterator struct {
	iter *pebble.Iterator
}

func (i *Iterator) Seek(key []byte) bool {
	return i.iter.SeekGE(key)
}

func (i *Iterator) Key() []byte {
	return i.iter.Key()
}

func (i *Iterator) Value() []byte {
	return i.iter.Value()
}

func (i *Iterator) First() bool {
	return i.iter.First()
}

func (i *Iterator) Last() bool {
	return i.iter.Last()
}

func (i *Iterator) Next() bool {
	return i.iter.Next()
}

func (i *Iterator) Prev() bool {
	return i.iter.Prev()
}
