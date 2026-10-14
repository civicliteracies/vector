use std::{
    sync::{Arc, Weak},
    time::Duration,
};

use tokio::time::timeout;
use tokio_test::{assert_pending, task::spawn};

use super::{FilesystemUnderTest, create_default_buffer_v2, read_next, read_next_some};
use crate::{
    test::{SizedRecord, acknowledge, with_temp_dir},
    variants::disk_v2::{BufferReader, BufferWriter, ledger::Ledger},
};

/// How long a read or a settle that should finish may take. Each finishes without waiting on any
/// timer, so this only bounds a test that would otherwise wait forever.
const PROMPT: Duration = Duration::from_secs(5);

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

/// Writes one single-event record per payload length, flushes, and returns each record's size.
async fn write_records(
    writer: &mut BufferWriter<SizedRecord, FilesystemUnderTest>,
    payload_lens: &[u32],
) -> Vec<u64> {
    let mut record_sizes = Vec::new();
    for &len in payload_lens {
        let bytes_written = writer
            .write_record(SizedRecord::new(len))
            .await
            .expect("write should not fail");
        record_sizes.push(bytes_written as u64);
    }
    writer.flush().await.expect("flush should not fail");
    record_sizes
}

/// Reads until the reader ends its input, each read bounded by `PROMPT`, and returns the records.
async fn read_to_end(
    reader: &mut BufferReader<SizedRecord, FilesystemUnderTest>,
) -> Vec<SizedRecord> {
    let mut records = Vec::new();
    while let Some(record) = timeout(PROMPT, read_next(reader))
        .await
        .expect("the reader should end its input once every record is read")
    {
        records.push(record);
    }
    records
}

/// A sink holds the records it read in its open batch until its input ends, and acknowledges them
/// only once it has delivered the batch. With its writer closed, the reader must end the input
/// once every record has been read, while those records are still unacknowledged: waiting for
/// their acknowledgements would wait on the sink, which is waiting on the end of its input.
#[tokio::test]
async fn reader_ends_once_every_record_is_read_while_records_are_unacknowledged() {
    with_temp_dir(|dir| {
        let data_dir = dir.to_path_buf();

        async move {
            let (mut writer, mut reader, ledger) =
                create_default_buffer_v2::<_, SizedRecord>(data_dir).await;
            let record_sizes = write_records(&mut writer, &[100, 101, 102]).await;
            writer.close();

            let held = read_to_end(&mut reader).await;
            assert_eq!(held.len(), 3);
            assert_eq!(
                ledger.get_total_buffer_size(),
                record_sizes.iter().sum::<u64>(),
                "the held records are unacknowledged and should still count toward the buffer size"
            );
            drop(held);
        }
    })
    .await;
}

/// The reader is waiting for the writer when a record is flushed and the writer closes. The
/// writer publishes the record's size before it marks itself done, so the reader that wakes sees
/// the record as unread and returns it before it ends its input.
#[tokio::test]
async fn a_record_flushed_just_before_the_close_is_read() {
    with_temp_dir(|dir| {
        let data_dir = dir.to_path_buf();

        async move {
            let (mut writer, mut reader, _ledger) =
                create_default_buffer_v2::<_, SizedRecord>(data_dir).await;
            write_records(&mut writer, &[100]).await;
            let first = read_next_some(&mut reader).await;

            let mut waiting = spawn(read_next(&mut reader));
            assert_pending!(waiting.poll());

            write_records(&mut writer, &[101]).await;
            writer.close();

            let second = timeout(PROMPT, waiting)
                .await
                .expect("the waiting read should wake once the writer flushes and closes");
            assert_eq!(second, Some(SizedRecord::new(101)));
            assert_eq!(read_to_end(&mut reader).await, Vec::new());
            drop(first);
        }
    })
    .await;
}

/// `settle` waits until every record the reader returned is acknowledged, applies the
/// acknowledgements to the ledger, and returns with the buffer empty.
#[tokio::test]
async fn settle_returns_once_every_returned_record_is_acknowledged() {
    with_temp_dir(|dir| {
        let data_dir = dir.to_path_buf();

        async move {
            let (mut writer, mut reader, ledger) =
                create_default_buffer_v2::<_, SizedRecord>(data_dir).await;
            write_records(&mut writer, &[100, 101]).await;
            writer.close();
            let mut held = read_to_end(&mut reader).await;
            let second = held.pop().expect("two records were written");
            let first = held.pop().expect("two records were written");
            let checkpoint = ledger.state().get_last_reader_record_id();

            let mut settling = spawn(reader.settle());
            assert_pending!(settling.poll());

            acknowledge(first).await;
            assert_pending!(
                settling.poll(),
                "one returned record is still unacknowledged"
            );

            acknowledge(second).await;
            timeout(PROMPT, settling)
                .await
                .expect("settle should return once every returned record is acknowledged")
                .expect("settle should not fail");
            assert_eq!(ledger.get_total_buffer_size(), 0);
            assert_eq!(ledger.state().get_last_reader_record_id(), checkpoint + 2);
        }
    })
    .await;
}

/// A buffer reopened after its reader settled holds nothing to read: the acknowledgements reached
/// the durable checkpoint.
#[tokio::test]
async fn reopening_after_a_settled_end_reads_nothing() {
    with_temp_dir(|dir| {
        let data_dir = dir.to_path_buf();

        async move {
            let (mut writer, mut reader, ledger) =
                create_default_buffer_v2::<_, SizedRecord>(data_dir.clone()).await;
            write_records(&mut writer, &[100, 101, 102]).await;
            writer.close();
            for record in read_to_end(&mut reader).await {
                acknowledge(record).await;
            }
            timeout(PROMPT, reader.settle())
                .await
                .expect("settle should return once every returned record is acknowledged")
                .expect("settle should not fail");

            let released = Arc::downgrade(&ledger);
            drop(reader);
            drop(writer);
            drop(ledger);
            wait_for_ledger_release(released).await;

            let (mut writer, mut reader, ledger) =
                create_default_buffer_v2::<_, SizedRecord>(data_dir).await;
            assert_eq!(ledger.get_total_buffer_size(), 0);
            writer.close();
            assert_eq!(read_to_end(&mut reader).await, Vec::new());
        }
    })
    .await;
}

/// Acknowledgements reach the ledger only through the reader. A reader dropped at the end of its
/// input without settling, as after a sink that returned an error, leaves the records it returned
/// in the buffer, acknowledged or not, and a reopened buffer reads each of them again: a missing
/// settle re-sends records and loses none.
#[tokio::test]
async fn reopening_after_an_unsettled_end_reads_the_returned_records_again() {
    with_temp_dir(|dir| {
        let data_dir = dir.to_path_buf();

        async move {
            let (mut writer, mut reader, ledger) =
                create_default_buffer_v2::<_, SizedRecord>(data_dir.clone()).await;
            let record_sizes = write_records(&mut writer, &[100, 101, 102]).await;
            writer.close();
            for record in read_to_end(&mut reader).await {
                acknowledge(record).await;
            }
            ledger.flush().expect("ledger flush should not fail");

            let released = Arc::downgrade(&ledger);
            drop(reader);
            drop(writer);
            drop(ledger);
            wait_for_ledger_release(released).await;

            let (mut writer, mut reader, ledger) =
                create_default_buffer_v2::<_, SizedRecord>(data_dir).await;
            assert_eq!(
                ledger.get_total_buffer_size(),
                record_sizes.iter().sum::<u64>()
            );
            writer.close();
            let read_again: Vec<u32> = read_to_end(&mut reader)
                .await
                .into_iter()
                .map(|record| record.0)
                .collect();
            assert_eq!(read_again, vec![100, 101, 102]);
        }
    })
    .await;
}
