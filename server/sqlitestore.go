// Copyright 2019-2022 The NATS Authors
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
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/iancoleman/strcase"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitemigration"
	"zombiezen.com/go/sqlite/sqlitex"
)

type SQLiteStoreConfig struct {
	// Where the parent directory for all storage will be located.
	StoreDir string
	// PageSize is the `pragma page_size` size.
	PageSize uint64
	// MaxPageCount is the `pragma max_page_count` size.
	MaxPageCount uint64
	// Cipher is the cipher to use when encrypting.
	Cipher StoreCipher
}

type sqliteStore struct {
	// fullPath string
	nextSeq       uint64
	deletedMsgs   int64
	maxMsgs       int64
	maxMsgAgeNano int64
	msgCount      int64
	firstSeq      uint64
	lastSeq       uint64
	ctx           context.Context
	db            *sqliteDatabase
}

func newSQLiteStore(ctx context.Context, storeCfg *SQLiteStoreConfig, streamCfg *StreamConfig) (*sqliteStore, error) {

	filename := fmt.Sprintf("/home/delaney/Desktop/nats_%s.sqlite", strcase.ToSnake(streamCfg.Name))

	// check if file exists, if so, delete it
	if _, err := os.Stat(filename); err == nil {
		os.Remove(filename)
	}

	// fullPath := filepath.Join(storeCfg.StoreDir, filename)
	db, err := newSQLiteDatabase(ctx, filename, []string{`
		CREATE TABLE IF NOT EXISTS messages (
			seq INTEGER PRIMARY KEY,
			subject TEXT NOT NULL,
			hdr BLOB NOT NULL,
			msg BLOB NOT NULL,
			ts INTEGER NOT NULL
		);		

		CREATE INDEX IF NOT EXISTS messages_subject_idx ON messages (subject);
		CREATE INDEX IF NOT EXISTS messages_ts_idx ON messages (ts);

		CREATE TABLE IF NOT EXISTS state (
			message_count INTEGER NOT NULL,
			first_seq INTEGER NOT NULL,
			last_seq INTEGER NOT NULL,
			max_messages INTEGER NOT NULL,
			max_age INTEGER NOT NULL,
			deleted_message_count INTEGER NOT NULL
		);

		INSERT INTO state (
			message_count,
			first_seq,
			last_seq,
			max_messages,
			max_age,
			deleted_message_count
		) 
		VALUES (
			0, 
			0, 
			0, 
			0,
			0,
			0
		);

		CREATE TABLE IF NOT EXISTS deleted_messages_gaps (
			start_inclusive_seq INTEGER NOT NULL,
			end_inclusive_seq INTEGER NOT NULL,
			PRIMARY KEY (start_inclusive_seq, end_inclusive_seq)
		);
	`})
	if err != nil {
		return nil, err
	}

	ss := &sqliteStore{
		ctx: ctx,
		db:  db,
	}

	var firstSeq, lastSeq, messageCount, maxAge, deletedMessageCount int64

	if err := db.WriteTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep(`SELECT * FROM state LIMIT 1`)
		defer stmt.Reset()

		if hadRows, err := stmt.Step(); err != nil {
			return fmt.Errorf("unable to get next sequence number: %v", err)
		} else if !hadRows {
			return fmt.Errorf("unable to get next sequence number: no rows")
		}
		firstSeq = stmt.GetInt64("first_seq")
		lastSeq = stmt.GetInt64("last_seq")
		messageCount = stmt.GetInt64("message_count")
		deletedMessageCount = stmt.GetInt64("deleted_msgs")
		maxMessages := stmt.GetInt64("max_messages")
		maxAge = stmt.GetInt64("max_age")

		if maxMessages != streamCfg.MaxMsgs {
			updateStmt := tx.Prep(`UPDATE state SET max_messages = $max_messages`)
			defer updateStmt.Reset()

			updateStmt.SetInt64("$max_messages", int64(streamCfg.MaxMsgs))
			if _, err := updateStmt.Step(); err != nil {
				return fmt.Errorf("unable to update max_messages: %v", err)
			}

			if streamCfg.MaxMsgs > 0 && lastSeq > streamCfg.MaxMsgs {
				deleteStmt := tx.Prep(`DELETE FROM messages WHERE seq < $seq`)
				defer deleteStmt.Reset()

				deleteStmt.SetInt64("$seq", int64(lastSeq-streamCfg.MaxMsgs))
				if _, err := deleteStmt.Step(); err != nil {
					return fmt.Errorf("unable to delete old messages: %v", err)
				}

				updateStmt := tx.Prep(`UPDATE state SET message_count = $message_count, first_seq = $first_seq`)
				defer updateStmt.Reset()

				updateStmt.SetInt64("$message_count", int64(streamCfg.MaxMsgs))
				updateStmt.SetInt64("$first_seq", int64(lastSeq-streamCfg.MaxMsgs+1))
				if _, err := updateStmt.Step(); err != nil {
					return fmt.Errorf("unable to update state: %v", err)
				}
			}
		}

		cfgNano := int64(streamCfg.MaxAge.Nanoseconds())
		if maxAge != cfgNano {
			updateStmt := tx.Prep(`UPDATE state SET max_age = $max_age`)
			defer updateStmt.Reset()

			maxAge = int64(streamCfg.MaxAge.Nanoseconds())
			updateStmt.SetInt64("$max_age", maxAge)
			if _, err := updateStmt.Step(); err != nil {
				return fmt.Errorf("unable to update max_age: %v", err)
			}
		}

		if err := ss.trimOlderThan(tx, cfgNano+time.Now().UnixNano()); err != nil {
			return fmt.Errorf("unable to trim messages: %v", err)
		}

		return nil

	}); err != nil {
		return nil, fmt.Errorf("unable to get next sequence number in transaction: %w", err)
	}
	ss.firstSeq = uint64(firstSeq)
	ss.lastSeq = uint64(lastSeq)
	ss.nextSeq = uint64(lastSeq + 1)
	ss.maxMsgs = streamCfg.MaxMsgs
	ss.msgCount = int64(messageCount)
	ss.maxMsgAgeNano = maxAge
	ss.deletedMsgs = int64(deletedMessageCount)

	return ss, nil
}

func (ss *sqliteStore) StoreMsg(subject string, hdr, msg []byte) (seq uint64, ts int64, err error) {
	ts = time.Now().UnixNano()
	seq = ss.nextSeq
	if err := ss.StoreRawMsg(subject, hdr, msg, seq, ts); err != nil {
		return 0, 0, err
	}
	return seq, ts, nil
}

// StoreRawMsg stores a raw message with expected sequence number and timestamp.
func (ss *sqliteStore) StoreRawMsg(subject string, hdr, msg []byte, seq uint64, tsNano int64) error {
	var possibleFirstSeq, possibleLastSeq, possibleNextSeq, possibleMsgCount uint64
	if err := ss.db.WriteTX(ss.ctx, func(tx *sqlite.Conn) error {
		insertStmt := tx.Prep(`
				INSERT INTO messages (
					seq, subject, hdr, msg, ts
				) VALUES (
					$seq, $subject, $hdr, $msg, $ts
				)
			`)
		defer insertStmt.Reset()
		insertStmt.SetInt64("$seq", int64(seq))
		insertStmt.SetText("$subject", subject)
		insertStmt.SetBytes("$hdr", hdr)
		insertStmt.SetBytes("$msg", msg)
		insertStmt.SetInt64("$ts", tsNano)
		if _, err := insertStmt.Step(); err != nil {
			return fmt.Errorf("unable to store message: %v", err)
		}

		stateUpdateStmt := tx.Prep(`UPDATE state SET message_count = message_count + 1, last_seq = $seq`)
		defer stateUpdateStmt.Reset()
		stateUpdateStmt.SetInt64("$seq", int64(seq))
		if _, err := stateUpdateStmt.Step(); err != nil {
			return fmt.Errorf("unable to update state: %v", err)
		}

		possibleLastSeq = seq
		possibleNextSeq = seq + 1
		possibleMsgCount = uint64(ss.msgCount + 1)

		if ss.maxMsgs > 0 && ss.msgCount > ss.maxMsgs {
			deleteStmt := tx.Prep(`DELETE FROM messages WHERE seq < $seq`)
			defer deleteStmt.Reset()

			possibleFirstSeq = possibleLastSeq - uint64(ss.maxMsgs) + 1
			possibleFirstSeqI64 := int64(possibleFirstSeq)
			deleteStmt.SetInt64("$seq", possibleFirstSeqI64)
			if _, err := deleteStmt.Step(); err != nil {
				return fmt.Errorf("unable to delete old messages: %v", err)
			}

			stateUpdateStmt := tx.Prep(`UPDATE state SET message_count = $message_count, first_seq = $first_seq`)
			defer stateUpdateStmt.Reset()

			stateUpdateStmt.SetInt64("$message_count", int64(ss.maxMsgs))
			stateUpdateStmt.SetInt64("$first_seq", possibleFirstSeqI64)
			if _, err := stateUpdateStmt.Step(); err != nil {
				return fmt.Errorf("unable to update state: %v", err)
			}
		}

		if err := ss.trimIfNeeded(tx); err != nil {
			return fmt.Errorf("unable to trim messages: %v", err)
		}

		return nil
	}); err != nil {
		return fmt.Errorf("unable to store raw message in transcation: %w", err)
	}

	ss.firstSeq = possibleFirstSeq
	ss.lastSeq = possibleLastSeq
	ss.nextSeq = uint64(possibleNextSeq)
	ss.msgCount = int64(possibleMsgCount)

	return nil
}

func (ss *sqliteStore) trimOlderThan(tx *sqlite.Conn, oldestMessageAllowed int64) error {
	deleteStmt := tx.Prep(`DELETE FROM messages WHERE ts < $ts`)
	defer deleteStmt.Reset()
	deleteStmt.SetInt64("$ts", oldestMessageAllowed)
	if _, err := deleteStmt.Step(); err != nil {
		return fmt.Errorf("unable to delete old messages: %v", err)
	}
	deletedCount := tx.Changes()

	messageCount := ss.msgCount - int64(deletedCount)
	firstSeq := ss.firstSeq + uint64(deletedCount)
	firstSeqI64 := int64(firstSeq)

	stateUpdateStmt := tx.Prep(`UPDATE state SET message_count = $messageCount, first_seq = $firstSeq`)
	defer stateUpdateStmt.Reset()
	stateUpdateStmt.SetInt64("$messageCount", int64(messageCount))
	stateUpdateStmt.SetInt64("$firstSeq", firstSeqI64)
	if _, err := stateUpdateStmt.Step(); err != nil {
		return fmt.Errorf("unable to update state: %v", err)
	}

	return nil
}

func (ss *sqliteStore) trimIfNeeded(tx *sqlite.Conn) error {
	if ss.maxMsgAgeNano > 0 {
		oldestMessageAllowed := time.Now().UnixNano() - int64(ss.maxMsgAgeNano)
		if err := ss.trimOlderThan(tx, oldestMessageAllowed); err != nil {
			return fmt.Errorf("unable to trim messages: %v", err)
		}
	}
	return nil
}

// SkipMsg will use the next sequence number but not store anything.
func (ss *sqliteStore) SkipMsg() uint64 {
	ss.nextSeq++
	return ss.nextSeq
}

// LoadMsg will lookup the message by sequence number and return it if found.
func (ss *sqliteStore) LoadMsg(seq uint64, sm *StoreMsg) (msg *StoreMsg, err error) {
	if err := ss.db.ReadTX(ss.ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep(`
			SELECT subject, hdr, msg, ts 
			FROM messages 
			WHERE seq = $seq
		`)
		defer stmt.Reset()

		stmt.SetInt64("$seq", int64(seq))
		if hadRows, err := stmt.Step(); err != nil {
			return fmt.Errorf("unable to load message: %v", err)
		} else if !hadRows {
			return fmt.Errorf("unable to load message: no rows")
		}

		hdrBuf := make([]byte, stmt.GetLen("hdr"))
		stmt.GetBytes("hdr", hdrBuf)

		msgBuf := make([]byte, stmt.GetLen("msg"))
		stmt.GetBytes("msg", msgBuf)

		msg = &StoreMsg{
			subj: stmt.GetText("subject"),
			hdr:  hdrBuf,
			msg:  msgBuf,
			ts:   stmt.GetInt64("ts"),
		}

		return nil
	}); err != nil {
		return nil, fmt.Errorf("unable to load message in transaction: %w", err)
	}

	return msg, nil
}

func (ss *sqliteStore) LoadNextMsg(filter string, wc bool, start uint64, smp *StoreMsg) (sm *StoreMsg, skip uint64, err error) {

	return nil, 0, fmt.Errorf("not implemented")
}

func (ss *sqliteStore) LoadLastMsg(subject string, sm *StoreMsg) (*StoreMsg, error) {
	return nil, fmt.Errorf("not implemented")
}

// RemoveMsg will remove the message from this store.
// Will return the number of bytes removed.
func (ss *sqliteStore) RemoveMsg(seq uint64) (bool, error) {

	possibleFirstSeq := ss.firstSeq
	possibleDeletedCount := ss.deletedMsgs
	possibleMsgCount := ss.msgCount

	if err := ss.db.WriteTX(ss.ctx, func(tx *sqlite.Conn) error {
		deleteStmt := tx.Prep(`
			DELETE FROM messages WHERE seq = $seq
		`)
		defer deleteStmt.Reset()
		deleteStmt.SetInt64("$seq", int64(seq))
		if _, err := deleteStmt.Step(); err != nil {
			return fmt.Errorf("unable to remove message: %v", err)
		}
		possibleDeletedCount += int64(tx.Changes())

		if possibleDeletedCount > 0 {
			possibleMsgCount--
			if seq == possibleFirstSeq {
				possibleFirstSeq++
			}

			existingGapStmt := tx.Prep(`
				SELECT start_inclusive_seq, end_inclusive_seq
				FROM deleted_messages_gaps 
				WHERE start_inclusive_seq = $seq+1 
				OR end_inclusive_seq = $seq-1
			`)
			defer existingGapStmt.Reset()

			existingGapStmt.SetInt64("$seq", int64(seq))
			gaps := make([][2]int64, 0, 2)
			for {
				if hadRows, err := existingGapStmt.Step(); err != nil {
					return fmt.Errorf("unable to check for existing gap: %v", err)
				} else if !hadRows {
					break
				} else {
					gaps = append(gaps, [2]int64{
						existingGapStmt.GetInt64("start_inclusive_seq"),
						existingGapStmt.GetInt64("end_inclusive_seq"),
					})
				}
			}

			if len(gaps) == 0 {
				insertStmt := tx.Prep(`
				INSERT INTO deleted_messages_gaps (start_inclusive_seq,end_inclusive_seq)
				VALUES ($seq,$seq)
			`)
				defer insertStmt.Reset()
				insertStmt.SetInt64("$seq", int64(seq))
				if _, err := insertStmt.Step(); err != nil {
					return fmt.Errorf("unable to insert deleted message: %v", err)
				}
			} else {
				if len(gaps) == 1 {
					start := gaps[0][0]
					end := gaps[0][1]

					if start == int64(seq)+1 {
						updateStmt := tx.Prep(`
						UPDATE deleted_messages_gaps
						SET start_inclusive_seq = $seq
						WHERE start_inclusive_seq = $start
					`)
						defer updateStmt.Reset()
						updateStmt.SetInt64("$seq", int64(seq))
						updateStmt.SetInt64("$start", start)
						if _, err := updateStmt.Step(); err != nil {
							return fmt.Errorf("unable to update deleted message: %v", err)
						}

					} else {
						updateStmt := tx.Prep(`
						UPDATE deleted_messages_gaps
						SET end_inclusive_seq = $seq
						WHERE end_inclusive_seq = $end
					`)
						defer updateStmt.Reset()
						updateStmt.SetInt64("$seq", int64(seq))
						updateStmt.SetInt64("$end", end)
						if _, err := updateStmt.Step(); err != nil {
							return fmt.Errorf("unable to update deleted message: %v", err)
						}
					}
				} else {
					// We have two gaps, merge them.
					start := gaps[0][0]
					end := gaps[1][1]

					deleteStmt := tx.Prep(`
						DELETE FROM deleted_messages_gaps
						WHERE start_inclusive_seq = $start
					`)
					defer deleteStmt.Reset()
					deleteStmt.SetInt64("$start", start)
					if _, err := deleteStmt.Step(); err != nil {
						return fmt.Errorf("unable to update deleted message: %v", err)
					}

					updateStmt := tx.Prep(`
						UPDATE deleted_messages_gaps
						SET start_inclusive_seq = $start
						WHERE end_inclusive_seq = $end
					`)
					defer updateStmt.Reset()
					updateStmt.SetInt64("$start", start)
					updateStmt.SetInt64("$end", end)
					if _, err := updateStmt.Step(); err != nil {
						return fmt.Errorf("unable to update deleted message: %v", err)
					}
				}
			}

			stateUpdateStmt := tx.Prep(`
				UPDATE state 
				SET message_count = $messageCount, 
				deleted_message_count = $deletedMessageCount,
				first_seq = $firstSeq
			`)
			defer stateUpdateStmt.Reset()
			stateUpdateStmt.SetInt64("$messageCount", possibleMsgCount)
			stateUpdateStmt.SetInt64("$firstSeq", int64(possibleFirstSeq))
			stateUpdateStmt.SetInt64("$deletedMessageCount", possibleDeletedCount)
			if _, err := stateUpdateStmt.Step(); err != nil {
				return fmt.Errorf("unable to update state: %v", err)
			}
		}

		return nil
	}); err != nil {
		return false, fmt.Errorf("unable to remove message in transaction: %w", err)
	}

	return true, nil
}

func (ss *sqliteStore) EraseMsg(seq uint64) (bool, error) {
	return ss.RemoveMsg(seq)
}

// Purge will remove all messages from this store.
// Will return the number of purged messages.
func (ss *sqliteStore) Purge() (uint64, error) {
	var removedCount uint64
	if err := ss.db.WriteTX(ss.ctx, func(tx *sqlite.Conn) error {
		deleteStmt := tx.Prep(`
			DELETE FROM messages
		`)
		defer deleteStmt.Reset()
		if _, err := deleteStmt.Step(); err != nil {
			return fmt.Errorf("unable to purge messages: %v", err)
		}
		removedCount = uint64(tx.Changes())

		stateUpdateStmt := tx.Prep(`UPDATE state SET message_count = 0, first_seq = 0`)
		defer stateUpdateStmt.Reset()
		if _, err := stateUpdateStmt.Step(); err != nil {
			return fmt.Errorf("unable to update state: %v", err)
		}

		return nil
	}); err != nil {
		return 0, fmt.Errorf("unable to purge messages in transaction: %w", err)
	}

	return removedCount, nil
}

// PurgeEx will remove messages based on subject filters, sequence and number of messages to keep.
// Will return the number of purged messages.
func (ss *sqliteStore) PurgeEx(subject string, seq, keep uint64) (uint64, error) {
	return 0, fmt.Errorf("not implemented")
}

// Compact will remove all messages from this store up to
// but not including the seq parameter.
// Will return the number of purged messages.
func (ss *sqliteStore) Compact(seq uint64) (uint64, error) {
	var purgedCount uint64
	if err := ss.db.WriteTX(ss.ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep(`
			DELETE FROM messages WHERE seq < $seq
		`)
		defer stmt.Reset()
		stmt.SetInt64("$seq", int64(seq))
		if _, err := stmt.Step(); err != nil {
			return fmt.Errorf("unable to compact messages: %v", err)
		}
		purgedCount = uint64(tx.Changes())

		stateUpdateStmt := tx.Prep(`UPDATE state SET message_count = $count, first_seq = $seq`)
		defer stateUpdateStmt.Reset()
		stateUpdateStmt.SetInt64("$count", int64(purgedCount))
		stateUpdateStmt.SetInt64("$seq", int64(seq))
		if _, err := stateUpdateStmt.Step(); err != nil {
			return fmt.Errorf("unable to update state: %v", err)
		}

		return nil
	}); err != nil {
		return 0, fmt.Errorf("unable to compact messages in transaction: %w", err)
	}

	ss.msgCount -= int64(purgedCount)
	ss.firstSeq = seq

	return purgedCount, nil
}

// Truncate will truncate a stream store up to and including seq. Sequence needs to be valid.
func (ss *sqliteStore) Truncate(seq uint64) error {
	if err := ss.db.WriteTX(ss.ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep(`
			DELETE FROM messages WHERE seq <= $seq
		`)
		defer stmt.Reset()

		stmt.SetInt64("$seq", int64(seq))
		if _, err := stmt.Step(); err != nil {
			return fmt.Errorf("unable to truncate messages: %v", err)
		}

		return nil
	}); err != nil {
		return fmt.Errorf("unable to truncate messages in transaction: %w", err)
	}

	return nil
}

// GetSeqFromTime looks for the first sequence number that has the message with >= timestamp.
func (ss *sqliteStore) GetSeqFromTime(t time.Time) (seq uint64) {
	if err := ss.db.ReadTX(ss.ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep(`
			SELECT seq FROM messages WHERE ts >= $ts 
			ORDER BY seq ASC LIMIT 1
		`)
		defer stmt.Reset()

		stmt.SetInt64("$ts", t.UnixNano())
		if _, err := stmt.Step(); err != nil {
			return fmt.Errorf("unable to get sequence from time: %v", err)
		}

		seq = uint64(stmt.GetInt64("seq"))

		return nil
	}); err != nil {
		return 0
	}

	return
}

// FilteredState will return the SimpleState associated with the filtered subject and a proposed starting sequence.
func (ss *sqliteStore) FilteredState(seq uint64, subject string) SimpleState {
	return SimpleState{}
}

// SubjectsState returns a map of SimpleState for all matching subjects.
func (ss *sqliteStore) SubjectsState(filterSubject string) map[string]SimpleState {
	return nil
}

// State retrieves the state from the state file.
// This is not expected to be called in high performance code, only on startup.
func (ss *sqliteStore) State() StreamState {
	state := StreamState{}
	if err := ss.db.ReadTX(ss.ctx, func(tx *sqlite.Conn) error {
		countStmt := tx.Prep(`
			SELECT message_count,first_seq,last_seq, deleted_message_count
			FROM state
		`)
		defer countStmt.Reset()
		if _, err := countStmt.Step(); err != nil {
			return fmt.Errorf("unable to get state: %v", err)
		}
		state.Msgs = uint64(countStmt.GetInt64("message_count"))
		state.FirstSeq = uint64(countStmt.GetInt64("first_seq"))
		state.LastSeq = uint64(countStmt.GetInt64("last_seq"))
		state.NumDeleted = int(countStmt.GetInt64("deleted_message_count"))

		deleteGapsStmt := tx.Prep(`
			SELECT start_inclusive_seq, end_inclusive_seq
			FROM deleted_messages_gaps
		`)
		defer deleteGapsStmt.Reset()

		for {
			if hadRows, err := deleteGapsStmt.Step(); err != nil {
				return fmt.Errorf("unable to get deleted messages: %v", err)
			} else if !hadRows {
				break
			}
			start := uint64(deleteGapsStmt.GetInt64("start_inclusive_seq"))
			end := uint64(deleteGapsStmt.GetInt64("end_inclusive_seq"))

			for i := start; i <= end; i++ {
				state.Deleted = append(state.Deleted, i)
			}
		}
		state.NumDeleted = len(state.Deleted)
		return nil
	}); err != nil {
		return StreamState{}
	}

	return state

}

// FastState will fill in state with only the following.
// Msgs, Bytes, First and Last Sequence and Time and NumDeleted.
func (ss *sqliteStore) FastState(*StreamState) {
	panic("not implemented")
}

// Type returns the type of the underlying store.
func (ss *sqliteStore) Type() StorageType {
	return SQLiteStorage
}

// RegisterStorageUpdates registers a callback for updates to storage changes.
// It will present number of messages and bytes as a signed integer and an
// optional sequence number of the message if a single.
func (ss *sqliteStore) RegisterStorageUpdates(StorageUpdateHandler) {
	panic("not implemented")
}

func (ss *sqliteStore) UpdateConfig(cfg *StreamConfig) error {
	return fmt.Errorf("not implemented")
}

func (ss *sqliteStore) Delete() error {
	return fmt.Errorf("not implemented")
}

func (ss *sqliteStore) Stop() (err error) {
	if ss.db != nil {
		err = ss.db.Close()
	}

	return
}

func (ss *sqliteStore) ConsumerStore(name string, cfg *ConsumerConfig) (ConsumerStore, error) {
	return nil, fmt.Errorf("not implemented")
}

func (ss *sqliteStore) AddConsumer(o ConsumerStore) error {
	return fmt.Errorf("not implemented")
}

func (ss *sqliteStore) RemoveConsumer(o ConsumerStore) error {
	return fmt.Errorf("not implemented")
}

// Stream our snapshot compression and tar.
func (ss *sqliteStore) Snapshot(deadline time.Duration, includeConsumers, checkMsgs bool) (*SnapshotResult, error) {
	return nil, fmt.Errorf("not implemented")
}

func (ss *sqliteStore) Utilization() (total, reported uint64, err error) {
	if err := ss.db.ReadTX(ss.ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep(`pragma page_count`)
		defer stmt.Reset()
		if hadRows, err := stmt.Step(); err != nil {
			return fmt.Errorf("unable to get page count: %v", err)
		} else if !hadRows {
			return fmt.Errorf("unable to get page count")
		}

		stmt = tx.Prep(`pragma page_size`)
		defer stmt.Reset()
		if hadRows, err := stmt.Step(); err != nil {
			return fmt.Errorf("unable to get page size: %v", err)
		} else if !hadRows {
			return fmt.Errorf("unable to get page size")
		}

		pageSize := uint64(stmt.GetInt64("page_size"))
		pageCount := uint64(stmt.GetInt64("page_count"))

		total = pageSize * pageCount
		reported = total

		return nil
	}); err != nil {
		return 0, 0, fmt.Errorf("unable to get utilization: %w", err)
	}

	return
}

type sqliteDatabase struct {
	write *sqlitex.Pool
	read  *sqlitex.Pool
}

func newSQLiteDatabase(ctx context.Context, dbFilename string, migrations []string) (*sqliteDatabase, error) {
	if err := os.MkdirAll(filepath.Dir(dbFilename), 0755); err != nil {
		return nil, fmt.Errorf("could not create database directory: %w", err)
	}

	uri := fmt.Sprintf("file:%s?_journal_mode=WAL&_synchronous=NORMAL", dbFilename)
	writePool, err := sqlitex.Open(uri, 0, 1)
	if err != nil {
		return nil, fmt.Errorf("could not open write pool: %w", err)
	}

	conn := writePool.Get(ctx)
	defer writePool.Put(conn)

	schema := sqlitemigration.Schema{
		Migrations: migrations,
	}

	if err := sqlitemigration.Migrate(ctx, conn, schema); err != nil {
		return nil, fmt.Errorf("could not migrate event store: %w", err)
	}

	readPool, err := sqlitex.Open(uri, 0, runtime.NumCPU())
	if err != nil {
		return nil, fmt.Errorf("could not open read pool: %w", err)
	}

	db := &sqliteDatabase{
		write: writePool,
		read:  readPool,
	}
	return db, nil
}

func (db *sqliteDatabase) Close() error {
	if err := db.write.Close(); err != nil {
		return fmt.Errorf("failed to close write pool: %w", err)
	}

	if err := db.read.Close(); err != nil {
		return fmt.Errorf("failed to close read pool: %w", err)
	}

	return nil
}

type TxFn func(tx *sqlite.Conn) error

func (db *sqliteDatabase) WriteTX(ctx context.Context, fn TxFn) (err error) {
	conn := db.write.Get(ctx)
	if conn == nil {
		return fmt.Errorf("could not get write connection from pool")
	}
	defer db.write.Put(conn)

	endFn, err := sqlitex.ImmediateTransaction(conn)
	if err != nil {
		return fmt.Errorf("could not start transaction: %w", err)
	}
	defer endFn(&err)

	if err := fn(conn); err != nil {
		return fmt.Errorf("could not execute write transaction: %w", err)
	}

	return nil
}

func (db *sqliteDatabase) ReadTX(ctx context.Context, fn TxFn) (err error) {
	conn := db.read.Get(ctx)
	if conn == nil {
		return fmt.Errorf("could not get read connection from pool")
	}
	defer db.read.Put(conn)

	endFn := sqlitex.Transaction(conn)
	defer endFn(&err)

	if err := fn(conn); err != nil {
		return fmt.Errorf("could not execute read transaction: %w", err)
	}

	return nil
}

const (
	secondsInADay      = 86400
	unixEpochJulianDay = 2440587.5
)

// timeToJulianDay converts a time.Time into a Julian day.
func timeToJulianDay(t time.Time) float64 {
	return float64(t.UTC().Unix())/secondsInADay + unixEpochJulianDay
}

// julianDayToTime converts a Julian day into a time.Time.
func julianDayToTime(d float64) time.Time {
	return time.Unix(int64((d-unixEpochJulianDay)*secondsInADay), 0).UTC()
}

func julianNow() float64 {
	return timeToJulianDay(time.Now())
}

func julianDayToTimeStmt(stmt *sqlite.Stmt, param string) time.Time {
	julianDays := stmt.GetFloat(param)
	return julianDayToTime(julianDays)
}
