package kv_pebble_test

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	kv_interface "github.com/xlander-io/kv"
	"github.com/xlander-io/kv_pebble"
)

func newDB() kv_interface.KVDB {
	const db_path = "./kv_pebble_test.db"
	os.RemoveAll(db_path)
	kvdb, err := kv_pebble.NewDB(db_path)
	if err != nil {
		fmt.Println("new db err:", err)
		return nil
	}
	return kvdb
}

func TestSimple(t *testing.T) {
	kvdb := newDB()
	defer kvdb.Close()

	//simple test
	kvdb.Put([]byte("key1"), []byte("val1"), false)
	kvdb.Put([]byte("key2"), []byte("val2"), true)
	kvdb.Put([]byte("key1"), []byte("val11"), false)

	//simple get
	{
		val, err := kvdb.Get([]byte("key1"))
		fmt.Println("kvdb key1 get :", string(val))

		if nil != err {
			t.Fatal(err)
		}

		if !bytes.Equal(val, []byte("val11")) {
			t.Fatalf("Unexpected value want: %s, got %s", []byte("val11"), val)
		}
	}

	{
		val, err := kvdb.Get([]byte("key2"))
		fmt.Println("kvdb key2 get :", string(val))
		if nil != err {
			t.Fatal(err)
		}

		if !bytes.Equal(val, []byte("val2")) {
			t.Fatalf("Unexpected value want: %s, got %s", []byte("val11"), val)
		}
	}

}

func TestBatch(t *testing.T) {
	kvdb := newDB()
	defer kvdb.Close()

	//batch test
	b := kv_interface.NewBatch()
	b.Put([]byte("key1"), []byte("val1"))
	b.Put([]byte("key1"), []byte("val11"))
	b.Put([]byte("key2"), []byte("val2"))
	b.Put([]byte("key3"), []byte("val3"))
	b.Put([]byte("key4"), []byte("val4"))
	b.Delete([]byte("key3"))

	kvdb.WriteBatch(b, true)

	{
		val, err := kvdb.Get([]byte("key1"))

		if nil != err {
			t.Fatal(err)
		}

		fmt.Println("kvdb key1 get :", string(val))

		if !bytes.Equal(val, []byte("val11")) {
			t.Fatalf("Unexpected value want: %s, got %s", []byte("val11"), val)
		}
	}

	{
		val, err := kvdb.Get([]byte("key2"))

		if nil != err {
			t.Fatal(err)
		}

		fmt.Println("kvdb key2 get :", string(val))

		if !bytes.Equal(val, []byte("val2")) {
			t.Fatalf("Unexpected value want: %s, got %s", []byte("val11"), val)
		}
	}

	{
		val, err := kvdb.Get([]byte("key3"))

		if nil == err {
			t.Fatal(err)
		}

		fmt.Println("kvdb key3 get :", string(val))
	}

	{
		val, err := kvdb.Get([]byte("key4"))

		if nil != err {
			t.Fatal(err)
		}

		fmt.Println("kvdb key4 get :", string(val))

		if !bytes.Equal(val, []byte("val4")) {
			t.Fatalf("Unexpected value want: %s, got %s", []byte("val4"), val)
		}
	}
}

func TestIterator(t *testing.T) {
	kvdb := newDB()
	defer kvdb.Close()

	const (
		key1     = "key1"
		key11    = "key11"
		key111   = "key111"
		key1111  = "key1111"
		key11111 = "key11111"
	)
	const (
		content1 = "content1"
		content2 = "content2"
		content3 = "content3"
		content4 = "content4"
		content5 = "content5"
	)

	kvdb.Put([]byte(key1), []byte(content1), false)
	kvdb.Put([]byte(key11), []byte(content2), true)
	kvdb.Put([]byte(key111), []byte(content3), false)
	kvdb.Put([]byte(key1111), []byte(content4), true)
	kvdb.Put([]byte(key11111), []byte(content5), true)

	iter := kvdb.NewIterator([]byte(key111), []byte(key11111))

	{
		ok := iter.Seek([]byte(key1)) //"keyaaa" "keybbb"

		if !ok {
			t.Fatal("Seek for key1 failed!")
		}

		key := iter.Key()
		value := iter.Value()
		fmt.Println(string(key), string(value))

		if !bytes.Equal(key, []byte(key111)) {
			t.Fatalf("Unexpected key want: %s, got %s", []byte(key111), key)
		}

		if !bytes.Equal(value, []byte(content3)) {
			t.Fatalf("Unexpected value want: %s, got %s", []byte(content3), value)
		}
	}

	{
		ok := iter.Next()

		if !ok {
			t.Fatal("Next for iterator failed!")
		}

		key := iter.Key()
		value := iter.Value()

		fmt.Println(string(key), string(value))

		if !bytes.Equal(key, []byte(key1111)) {
			t.Fatalf("Unexpected key want: %s, got %s", []byte(key1111), key)
		}

		if !bytes.Equal(value, []byte(content4)) {
			t.Fatalf("Unexpected value want: %s, got %s", []byte(content4), value)
		}
	}

	{
		ok := iter.Next()

		if ok {
			t.Fatal("Next for iterator should be failed!")
		}
	}

	{
		ok := iter.Prev()

		if !ok {
			t.Fatal("Prev for iterator failed!")
		}

		key := iter.Key()
		value := iter.Value()

		fmt.Println(string(key), string(value))

		if !bytes.Equal(key, []byte(key1111)) {
			t.Fatalf("Unexpected key want: %s, got %s", []byte(key1111), key)
		}

		if !bytes.Equal(value, []byte(content4)) {
			t.Fatalf("Unexpected value want: %s, got %s", []byte(content4), value)
		}
	}

	{
		ok := iter.Prev()

		if !ok {
			t.Fatal("Prev for iterator failed!")
		}

		key := iter.Key()
		value := iter.Value()

		fmt.Println(string(key), string(value))

		if !bytes.Equal(key, []byte(key111)) {
			t.Fatalf("Unexpected key want: %s, got %s", []byte(key111), key)
		}

		if !bytes.Equal(value, []byte(content3)) {
			t.Fatalf("Unexpected value want: %s, got %s", []byte(content3), value)
		}
	}

	{
		ok := iter.Prev()

		if ok {
			t.Fatal("Prev for iterator should be failed!")
		}
	}

	{
		ok := iter.First()

		if !ok {
			t.Fatal("First for iterator failed!")
		}

		key := iter.Key()
		value := iter.Value()

		fmt.Println(string(key), string(value))

		if !bytes.Equal(key, []byte(key111)) {
			t.Fatalf("Unexpected key want: %s, got %s", []byte(key111), key)
		}

		if !bytes.Equal(value, []byte(content3)) {
			t.Fatalf("Unexpected value want: %s, got %s", []byte(content3), value)
		}
	}

	{
		ok := iter.Prev()

		if ok {
			t.Fatal("Prev for iterator should be failed!")
		}
	}

	{
		ok := iter.Last()

		if !ok {
			t.Fatal("Last for iterator failed!")
		}

		key := iter.Key()
		value := iter.Value()

		fmt.Println(string(key), string(value))

		if !bytes.Equal(key, []byte(key1111)) {
			t.Fatalf("Unexpected key want: %s, got %s", []byte(key1111), key)
		}

		if !bytes.Equal(value, []byte(content4)) {
			t.Fatalf("Unexpected value want: %s, got %s", []byte(content4), value)
		}
	}

	{
		ok := iter.Next()

		if ok {
			t.Fatal("Next for iterator should be failed!")
		}
	}

}
