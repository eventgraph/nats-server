// Copyright 2019-2023 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"
)

func defaultStreamConfig() *StreamConfig {
	return &StreamConfig{}
}

type streamStoreTest func(t *testing.T, store StreamStore)

func runStreamStoreTest(t *testing.T, streamCfg *StreamConfig, fn streamStoreTest) (err error) {
	// msConfig := *streamCfg
	// msConfig.Storage = MemoryStorage
	// memoryStore, err := newMemStore(&msConfig)
	// if err != nil {
	// 	return fmt.Errorf("Unexpected error creating memory store: %v", err)
	// }

	// fsConfig := *streamCfg
	// fsConfig.Storage = FileStorage
	// fsConfig.Name = "ZZZ"
	// fileStore, err := newFileStore(FileStoreConfig{StoreDir: t.TempDir()}, fsConfig)
	// if err != nil {
	// 	t.Fatalf("Unexpected error: %v", err)
	// }

	ctx := context.Background()

	ssConfig := *streamCfg
	ssConfig.Storage = SQLiteStorage
	ssConfig.Name = "YYY"
	sqliteStore, err := newSQLiteStore(ctx, &SQLiteStoreConfig{StoreDir: t.TempDir()}, &ssConfig)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	stores := []StreamStore{
		sqliteStore,
		// memoryStore,
		// fileStore,
	}

	for _, store := range stores {
		defer store.Stop()
		fn(t, store)
	}

	return
}

func TestStreamStoreBasics(t *testing.T) {
	streamCfg := &StreamConfig{MaxMsgs: 10}
	runStreamStoreTest(t, streamCfg, func(t *testing.T, store StreamStore) {
		subj, msg := "foo", []byte("Hello World")
		now := time.Now().UnixNano()
		if seq, ts, err := store.StoreMsg(subj, nil, msg); err != nil {
			t.Fatalf("Error storing msg: %v", err)
		} else if seq != 1 {
			t.Fatalf("Expected sequence to be 1, got %d", seq)
		} else if ts < now || ts > now+int64(time.Millisecond) {
			t.Fatalf("Expected timestamp to be current, got %v", ts-now)
		}

		state := store.State()
		if state.Msgs != 1 {
			t.Fatalf("Expected 1 msg, got %d", state.Msgs)
		}
		sm, err := store.LoadMsg(1, nil)
		if err != nil {
			t.Fatalf("Unexpected error looking up msg: %v", err)
		}
		if sm.subj != subj {
			t.Fatalf("Subjects don't match, original %q vs %q", subj, sm.subj)
		}
		if !bytes.Equal(sm.msg, msg) {
			t.Fatalf("Msgs don't match, original %q vs %q", msg, sm.msg)
		}
	})

}

func TestStreamStoreMsgLimit(t *testing.T) {
	streamCfg := &StreamConfig{MaxMsgs: 10}
	runStreamStoreTest(t, streamCfg, func(t *testing.T, store StreamStore) {
		subj, msg := "foo", []byte("Hello World")
		for i := 0; i < 10; i++ {
			store.StoreMsg(subj, nil, msg)
		}
		state := store.State()
		if state.Msgs != 10 {
			t.Fatalf("Expected %d msgs, got %d", 10, state.Msgs)
		}
		if _, _, err := store.StoreMsg(subj, nil, msg); err != nil {
			t.Fatalf("Error storing msg: %v", err)
		}
		state = store.State()
		if state.Msgs != 10 {
			t.Fatalf("Expected %d msgs, got %d", 10, state.Msgs)
		}
		if state.LastSeq != 11 {
			t.Fatalf("Expected the last sequence to be 11 now, but got %d", state.LastSeq)
		}
		if state.FirstSeq != 2 {
			t.Fatalf("Expected the first sequence to be 2 now, but got %d", state.FirstSeq)
		}
		// Make sure we can not lookup seq 1.
		if _, err := store.LoadMsg(1, nil); err == nil {
			t.Fatalf("Expected error looking up seq 1 but got none")
		}
	})
}

func TestStreamStoreBytesLimit(t *testing.T) {
	subj, msg := "foo", make([]byte, 512)
	toStore := uint64(1024)
	streamCfg := &StreamConfig{
		Storage:  MemoryStorage,
		MaxBytes: int64(8 * len(msg) * int(toStore)), // TODO No call to interface to support this yet
	}

	runStreamStoreTest(t, streamCfg, func(t *testing.T, store StreamStore) {
		t.Skip("Need to fix this test")
		for i := uint64(0); i < toStore; i++ {
			store.StoreMsg(subj, nil, msg)
		}
		state := store.State()
		if state.Msgs != toStore {
			t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
		}

		// Now send 10 more and check that bytes limit enforced.
		for i := 0; i < 10; i++ {
			if _, _, err := store.StoreMsg(subj, nil, msg); err != nil {
				t.Fatalf("Error storing msg: %v", err)
			}
		}
		state = store.State()
		if state.Msgs != toStore {
			t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
		}
		if state.FirstSeq != 11 {
			t.Fatalf("Expected first sequence to be 11, got %d", state.FirstSeq)
		}
		if state.LastSeq != toStore+10 {
			t.Fatalf("Expected last sequence to be %d, got %d", toStore+10, state.LastSeq)
		}
	})
}

func TestStreamStoreAgeLimit(t *testing.T) {
	maxAge := 10 * time.Millisecond
	streamCfg := &StreamConfig{MaxAge: maxAge}
	runStreamStoreTest(t, streamCfg, func(t *testing.T, store StreamStore) {
		// Store some messages. Does not really matter how many.
		subj, msg := "foo", []byte("Hello World")
		toStore := 100
		for i := 0; i < toStore; i++ {
			store.StoreMsg(subj, nil, msg)
		}
		state := store.State()
		if state.Msgs != uint64(toStore) {
			t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
		}
		checkExpired := func(t *testing.T) {
			t.Helper()
			checkFor(t, time.Second, maxAge, func() error {
				state = store.State()
				if state.Msgs != 0 {
					return fmt.Errorf("Expected no msgs, got %d", state.Msgs)
				}
				if state.Bytes != 0 {
					return fmt.Errorf("Expected no bytes, got %d", state.Bytes)
				}
				return nil
			})
		}
		// Let them expire
		checkExpired(t)
		// Now add some more and make sure that timer will fire again.
		for i := 0; i < toStore; i++ {
			store.StoreMsg(subj, nil, msg)
		}
		state = store.State()
		if state.Msgs != uint64(toStore) {
			t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
		}
		checkExpired(t)
	})
}

func TestStreamStoreTimeStamps(t *testing.T) {
	runStreamStoreTest(t, defaultStreamConfig(), func(t *testing.T, store StreamStore) {
		last := time.Now().UnixNano()
		subj, msg := "foo", []byte("Hello World")
		for i := 0; i < 10; i++ {
			time.Sleep(5 * time.Microsecond)
			store.StoreMsg(subj, nil, msg)
		}
		var smv StoreMsg
		for seq := uint64(1); seq <= 10; seq++ {
			sm, err := store.LoadMsg(seq, &smv)
			if err != nil {
				t.Fatalf("Unexpected error looking up msg: %v", err)
			}
			// These should be different
			if sm.ts <= last {
				t.Fatalf("Expected different timestamps, got %v", sm.ts)
			}
			last = sm.ts
		}
	})
}

func TestStreamStorePurge(t *testing.T) {
	runStreamStoreTest(t, defaultStreamConfig(), func(t *testing.T, store StreamStore) {
		subj, msg := "foo", []byte("Hello World")
		for i := 0; i < 10; i++ {
			store.StoreMsg(subj, nil, msg)
		}
		if state := store.State(); state.Msgs != 10 {
			t.Fatalf("Expected 10 msgs, got %d", state.Msgs)
		}
		store.Purge()
		if state := store.State(); state.Msgs != 0 {
			t.Fatalf("Expected no msgs, got %d", state.Msgs)
		}
	})
}

func TestStreamStoreCompact(t *testing.T) {
	runStreamStoreTest(t, defaultStreamConfig(), func(t *testing.T, store StreamStore) {
		subj, msg := "foo", []byte("Hello World")
		for i := 0; i < 10; i++ {
			store.StoreMsg(subj, nil, msg)
		}
		if state := store.State(); state.Msgs != 10 {
			t.Fatalf("Expected 10 msgs, got %d", state.Msgs)
		}
		n, err := store.Compact(6)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if n != 5 {
			t.Fatalf("Expected to have purged 5 msgs, got %d", n)
		}
		state := store.State()
		if state.Msgs != 5 {
			t.Fatalf("Expected 5 msgs, got %d", state.Msgs)
		}
		if state.FirstSeq != 6 {
			t.Fatalf("Expected first seq of 6, got %d", state.FirstSeq)
		}
		// Now test that compact will also reset first if seq > last
		n, err = store.Compact(100)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if n != 5 {
			t.Fatalf("Expected to have purged 5 msgs, got %d", n)
		}
		if state = store.State(); state.FirstSeq != 100 {
			t.Fatalf("Expected first seq of 100, got %d", state.FirstSeq)
		}
	})
}

func TestStreamStoreEraseMsg(t *testing.T) {
	runStreamStoreTest(t, defaultStreamConfig(), func(t *testing.T, store StreamStore) {
		subj, msg := "foo", []byte("Hello World")
		store.StoreMsg(subj, nil, msg)
		sm, err := store.LoadMsg(1, nil)
		if err != nil {
			t.Fatalf("Unexpected error looking up msg: %v", err)
		}
		if !bytes.Equal(msg, sm.msg) {
			t.Fatalf("Expected same msg, got %q vs %q", sm.msg, msg)
		}
		if removed, _ := store.EraseMsg(1); !removed {
			t.Fatalf("Expected erase msg to return success")
		}
	})
}

func TestStreamStoreMsgHeaders(t *testing.T) {
	runStreamStoreTest(t, defaultStreamConfig(), func(t *testing.T, store StreamStore) {
		subj, hdr, msg := "foo", []byte("name:derek"), []byte("Hello World")
		// FIXME: This is not correct, need to fix.
		// if sz := int(StreamStoreMsgSize(subj, hdr, msg)); sz != (len(subj) + len(hdr) + len(msg) + 16) {
		// 	t.Fatalf("Wrong size for stored msg with header")
		// }
		store.StoreMsg(subj, hdr, msg)
		sm, err := store.LoadMsg(1, nil)
		if err != nil {
			t.Fatalf("Unexpected error looking up msg: %v", err)
		}
		if !bytes.Equal(msg, sm.msg) {
			t.Fatalf("Expected same msg, got %q vs %q", sm.msg, msg)
		}
		if !bytes.Equal(hdr, sm.hdr) {
			t.Fatalf("Expected same hdr, got %q vs %q", sm.hdr, hdr)
		}
		if removed, _ := store.EraseMsg(1); !removed {
			t.Fatalf("Expected erase msg to return success")
		}
	})
}

func TestStreamStoreStreamStateDeleted(t *testing.T) {
	runStreamStoreTest(t, defaultStreamConfig(), func(t *testing.T, store StreamStore) {
		subj, toStore := "foo", uint64(10)
		for i := uint64(1); i <= toStore; i++ {
			msg := []byte(fmt.Sprintf("[%08d] Hello World!", i))
			if _, _, err := store.StoreMsg(subj, nil, msg); err != nil {
				t.Fatalf("Error storing msg: %v", err)
			}
		}
		state := store.State()
		if len(state.Deleted) != 0 {
			t.Fatalf("Expected deleted to be empty")
		}
		// Now remove some interior messages.
		var expected []uint64
		for seq := uint64(2); seq < toStore; seq += 2 {
			store.RemoveMsg(seq)
			expected = append(expected, seq)
		}
		state = store.State()
		if !reflect.DeepEqual(state.Deleted, expected) {
			t.Fatalf("Expected deleted to be %+v, got %+v\n", expected, state.Deleted)
		}
		// Now fill the gap by deleting 1 and 3
		store.RemoveMsg(1)
		store.RemoveMsg(3)
		expected = expected[2:]
		state = store.State()
		if !reflect.DeepEqual(state.Deleted, expected) {
			t.Fatalf("Expected deleted to be %+v, got %+v\n", expected, state.Deleted)
		}
		if state.FirstSeq != 5 {
			t.Fatalf("Expected first seq to be 5, got %d", state.FirstSeq)
		}
		store.Purge()
		if state = store.State(); len(state.Deleted) != 0 {
			t.Fatalf("Expected no deleted after purge, got %+v\n", state.Deleted)
		}
	})
}

func TestStreamStoreStreamTruncate(t *testing.T) {
	runStreamStoreTest(t, defaultStreamConfig(), func(t *testing.T, store StreamStore) {

		subj, toStore := "foo", uint64(100)
		for i := uint64(1); i <= toStore; i++ {
			if _, _, err := store.StoreMsg(subj, nil, []byte("ok")); err != nil {
				t.Fatalf("Error storing msg: %v", err)
			}
		}
		if state := store.State(); state.Msgs != toStore {
			t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
		}

		// Check that sequence has to be interior.
		if err := store.Truncate(toStore + 1); err != ErrInvalidSequence {
			t.Fatalf("Expected err of '%v', got '%v'", ErrInvalidSequence, err)
		}

		tseq := uint64(50)
		if err := store.Truncate(tseq); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if state := store.State(); state.Msgs != tseq {
			t.Fatalf("Expected %d msgs, got %d", tseq, state.Msgs)
		}

		// Now make sure we report properly if we have some deleted interior messages.
		store.RemoveMsg(10)
		store.RemoveMsg(20)
		store.RemoveMsg(30)
		store.RemoveMsg(40)

		tseq = uint64(25)
		if err := store.Truncate(tseq); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		expected := []uint64{10, 20}
		if state := store.State(); !reflect.DeepEqual(state.Deleted, expected) {
			t.Fatalf("Expected deleted to be %+v, got %+v\n", expected, state.Deleted)
		}
	})
}

func TestStreamStorePurgeExWithSubject(t *testing.T) {
	runStreamStoreTest(t, defaultStreamConfig(), func(t *testing.T, store StreamStore) {
		t.Skip("Need to fix this test, not sure why filestore is failing")

		for i := 0; i < 100; i++ {
			_, _, err := store.StoreMsg("foo", nil, nil)
			require_NoError(t, err)
		}

		// This should purge all.
		store.PurgeEx("foo", 1, 0)
		require_True(t, store.State().Msgs == 0)
	})
}

func TestStreamStoreUpdateMaxMsgsPerSubject(t *testing.T) {
	streamCfg := &StreamConfig{
		Name:       "TEST",
		Subjects:   []string{"foo"},
		MaxMsgsPer: 10,
	}

	runStreamStoreTest(t, streamCfg, func(t *testing.T, store StreamStore) {
		// Make sure this is honored on an update.
		streamCfg.MaxMsgsPer = 50
		err := store.UpdateConfig(streamCfg)
		require_NoError(t, err)

		numStored := 22
		for i := 0; i < numStored; i++ {
			_, _, err = store.StoreMsg("foo", nil, nil)
			require_NoError(t, err)
		}

		ss := store.SubjectsState("foo")["foo"]
		if ss.Msgs != uint64(numStored) {
			t.Fatalf("Expected to have %d stored, got %d", numStored, ss.Msgs)
		}

		// Now make sure we trunk if setting to lower value.
		streamCfg.MaxMsgsPer = 10
		err = store.UpdateConfig(streamCfg)
		require_NoError(t, err)

		ss = store.SubjectsState("foo")["foo"]
		if ss.Msgs != 10 {
			t.Fatalf("Expected to have %d stored, got %d", 10, ss.Msgs)
		}
	})
}
