use std::{
    sync::{Arc, Weak},
    time::Duration,
};

use tokio::time::timeout;
use tokio_test::{assert_pending, assert_ready, task::spawn};
use vector_common::finalization::{BatchNotifier, EventFinalizer, EventStatus};

use super::{FilesystemUnderTest, create_default_buffer_v2, read_next_some};
use crate::{
    buffer_usage_data::BufferUsageHandle,
    test::{SizedRecord, acknowledge as acknowledge_record, with_temp_dir},
    variants::disk_v2::{DiskBufferConfigBuilder, ledger::Ledger},
};

pub(crate) async fn acknowledge(batch: BatchNotifier) {
    let finalizer = EventFinalizer::new(batch);
    finalizer.update_status(EventStatus::Delivered);
    drop(finalizer); // This sends the status update
    tokio::task::yield_now().await;
}

#[tokio::test]
async fn ack_updates_ledger_correctly() {
    with_temp_dir(|dir| {
        let data_dir = dir.to_path_buf();

        async move {
            // Create a standalone ledger.
            let usage_handle = BufferUsageHandle::noop();
            let config = DiskBufferConfigBuilder::from_path(data_dir)
                .build()
                .expect("creating buffer should not fail");
            let ledger = Ledger::load_or_create(config, usage_handle)
                .await
                .expect("ledger should not fail to load/create");
            assert_eq!(ledger.consume_pending_acks(), 0);

            // Create our ledger, and make sure it's empty.
            let ledger = Arc::new(ledger);
            let finalizer = Arc::clone(&ledger).spawn_finalizer();
            assert_eq!(ledger.consume_pending_acks(), 0);

            // Now make sure it updates pending acks.
            let (batch, receiver) = BatchNotifier::new_with_receiver();
            finalizer.add(42, receiver);
            acknowledge(batch).await;
            assert_eq!(ledger.consume_pending_acks(), 42);
            assert_eq!(ledger.consume_pending_acks(), 0);
        }
    })
    .await;
}

#[tokio::test]
async fn ack_wakes_reader() {
    with_temp_dir(|dir| {
        let data_dir = dir.to_path_buf();

        async move {
            // Create a standalone ledger.
            let usage_handle = BufferUsageHandle::noop();
            let config = DiskBufferConfigBuilder::from_path(data_dir)
                .build()
                .expect("creating buffer should not fail");
            let ledger = Ledger::load_or_create(config, usage_handle)
                .await
                .expect("ledger should not fail to load/create");

            // Create our ledger, as well as a future for awaiting
            // writer progress, and make sure it's not yet woken up.
            let ledger = Arc::new(ledger);
            let finalizer = Arc::clone(&ledger).spawn_finalizer();

            let mut wait_for_writer = spawn(ledger.wait_for_writer());
            assert_pending!(wait_for_writer.poll());
            assert!(!wait_for_writer.is_woken());

            // Now fire off an acknowledgement, and make sure our call woke up and can complete.
            let (batch, receiver) = BatchNotifier::new_with_receiver();
            finalizer.add(1, receiver);
            acknowledge(batch).await;

            assert!(wait_for_writer.is_woken());
            assert_ready!(wait_for_writer.poll());
        }
    })
    .await;
}

/// Waits until every task holding the ledger has finished, so the buffer's lock file is released
/// and the buffer can be reopened in the same process.
async fn wait_for_ledger_release(ledger: Weak<Ledger<FilesystemUnderTest>>) {
    timeout(Duration::from_secs(10), async {
        while ledger.strong_count() > 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("ledger should be released once the reader and its records are dropped");
}

/// On reopen, the reader replays the already-acknowledged records at the start of its current
/// data file to reach its checkpoint, and discards them. Discarding a replayed record must not
/// produce an acknowledgement: one would be credited to the next records read after the
/// checkpoint, so records still held by a sink would count as delivered, leave the buffer size,
/// and move the durable checkpoint past them.
#[tokio::test]
async fn replayed_records_do_not_acknowledge_records_read_after_reopen() {
    with_temp_dir(|dir| {
        let data_dir = dir.to_path_buf();

        async move {
            // Eight single-event records in one data file. Each payload length identifies its
            // record: record N (1-based) has payload length 99 + N.
            let payload_lens: Vec<u32> = (100..108).collect();
            let (mut writer, mut reader, ledger) =
                create_default_buffer_v2::<_, SizedRecord>(data_dir.clone()).await;
            let mut record_sizes = Vec::new();
            for &len in &payload_lens {
                let bytes_written = writer
                    .write_record(SizedRecord::new(len))
                    .await
                    .expect("write should not fail");
                record_sizes.push(bytes_written as u64);
            }
            writer.flush().await.expect("flush should not fail");

            // First run: read and acknowledge records 1-3. Reading record 4 makes the reader
            // consume those acknowledgements and advance its checkpoint past record 3. Record 4
            // is never acknowledged in this run.
            for _ in 0..3 {
                let record = read_next_some(&mut reader).await;
                acknowledge_record(record).await;
            }
            let record_4 = read_next_some(&mut reader).await;
            assert_eq!(record_4, SizedRecord::new(103));
            let checkpoint = ledger.state().get_last_reader_record_id();
            assert_eq!(
                ledger.get_total_buffer_size(),
                record_sizes[3..].iter().sum::<u64>(),
                "records 1-3 should have left the buffer"
            );
            ledger.flush().expect("ledger flush should not fail");

            let released = Arc::downgrade(&ledger);
            drop(reader);
            drop(record_4);
            drop(writer);
            drop(ledger);
            wait_for_ledger_release(released).await;

            // Second run: the reader replays records 1-3 to reach its checkpoint, then resumes at
            // record 4. Hold records 4, 5 and 6 as an open sink batch would, and acknowledge only
            // record 4.
            let (writer, mut reader, ledger) =
                create_default_buffer_v2::<_, SizedRecord>(data_dir.clone()).await;
            assert_eq!(ledger.state().get_last_reader_record_id(), checkpoint);
            assert_eq!(
                ledger.get_total_buffer_size(),
                record_sizes[3..].iter().sum::<u64>()
            );

            let record_4 = read_next_some(&mut reader).await;
            let record_5 = read_next_some(&mut reader).await;
            let record_6 = read_next_some(&mut reader).await;
            assert_eq!(record_4, SizedRecord::new(103));
            assert_eq!(record_5, SizedRecord::new(104));
            assert_eq!(record_6, SizedRecord::new(105));
            acknowledge_record(record_4).await;

            // Give the finalizer task every chance to deliver whatever acknowledgements it holds,
            // then read record 7 so the reader consumes them.
            for _ in 0..16 {
                tokio::task::yield_now().await;
            }
            let record_7 = read_next_some(&mut reader).await;
            assert_eq!(record_7, SizedRecord::new(106));

            assert_eq!(
                ledger.state().get_last_reader_record_id(),
                checkpoint + 1,
                "only record 4 was acknowledged, so the checkpoint should advance past record 4 \
                 and no further"
            );
            assert_eq!(
                ledger.get_total_buffer_size(),
                record_sizes[4..].iter().sum::<u64>(),
                "records 5-8 are unacknowledged and should still count toward the buffer size"
            );
            ledger.flush().expect("ledger flush should not fail");

            // Stop without acknowledging records 5-7, as a crash would.
            let released = Arc::downgrade(&ledger);
            drop(reader);
            drop((record_5, record_6, record_7));
            drop(writer);
            drop(ledger);
            wait_for_ledger_release(released).await;

            // Third run: every unacknowledged record is delivered again, starting at record 5.
            let (_writer, mut reader, ledger) =
                create_default_buffer_v2::<_, SizedRecord>(data_dir).await;
            assert_eq!(ledger.state().get_last_reader_record_id(), checkpoint + 1);
            let mut redelivered = Vec::new();
            for _ in 0..4 {
                let record = read_next_some(&mut reader).await;
                redelivered.push(record.0);
                acknowledge_record(record).await;
            }
            assert_eq!(redelivered, vec![104, 105, 106, 107]);
        }
    })
    .await;
}
