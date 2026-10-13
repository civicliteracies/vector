# Vector, with three disk-buffer fixes

This branch is an upstream [Vector](https://github.com/vectordotdev/vector) release with the
patches below on top, and the tooling that builds and publishes it. It exists to produce a Vector
binary that carries the fixes until an upstream release does; nothing else about Vector is
changed. Each branch `eiti/v<version>` is upstream's tag `v<version>` plus the patch commits and
one tooling commit.

The patches are ordinary commits on Vector's own code, each with a regression test in the crate it
changes, so they can be offered upstream as they are.

## The patches

### Replayed disk-buffer records acknowledge nothing

Upstream issue: [vectordotdev/vector#26564](https://github.com/vectordotdev/vector/issues/26564).

When a disk buffer is reopened, its reader re-reads the already-delivered records at the start of
its current data file to reach its checkpoint, and discards them. Upstream attaches a delivery
notifier to every record it reads, including these, but registers no acknowledgement marker for
them. When the replayed records are dropped, their acknowledgements stay in the buffer as credit
and are paid to the next records read after the checkpoint. Those records then count as delivered
while a sink still holds them in an open batch: their bytes leave the buffer's size and the durable
checkpoint moves past them. After any restart, on upstream:

- a Vector killed before those batches upload does not replay them, so their rows are lost;
- a stopping Vector sees an empty buffer at its first upload, ends its input, and flushes every
  open partition at once instead of waiting for each batch's own timer.

The patch attaches the notifier only when the reader is ready, the condition under which it
registers a marker. Only the replay reads before the reader is ready, and it discards what it
reads, so no record that reaches a sink loses its notifier.

Test: `replayed_records_do_not_acknowledge_records_read_after_reopen` (`vector-buffers`).

### Bytes reported flushed have landed in the data file

Upstream issue: none filed for this cause yet;
[vectordotdev/vector#23605](https://github.com/vectordotdev/vector/issues/23605) reports events
that stay in a buffer once their source has finished, which may be this defect.

A disk buffer's writer hands a record to its data file, publishes the record's bytes to the ledger
and wakes the reader. The data file is a `tokio::fs::File`, which accepts a write and carries it
out later on a blocking thread, so the reader can be woken before the bytes are in the file. It
then reads nothing and waits for the writer again, and nothing wakes it until the next record is
written to that sink or Vector stops. Measured on aarch64 Linux, alternating one start-up burst of
80 rows on a build without the change and on one with it, each under a syscall trace with four
busy loops beside it: the burst was held in 8 of 98 runs without the change and in 0 of 98 with it.

The patch waits for the data file's own flush before the writer reports bytes as flushed, on both
paths that report them: the flush of the write buffer (the path the measurement exercised) and the
direct write of a record at least as large as the write buffer.

Tests: `bytes_reported_flushed_have_landed_in_the_file` and
`a_record_written_past_the_write_buffer_has_landed_when_it_is_reported_flushed` (`vector-buffers`),
which hold a writer to a file whose writes land only at its flush.

### A disk buffer ends its sink's input once nothing is left to read

Upstream issue: [vectordotdev/vector#25774](https://github.com/vectordotdev/vector/issues/25774).

When Vector stops, each source closes its writer into the buffers downstream. A disk buffer's
reader then ends its sink's input only once the buffer is empty, which is once every record it
returned has been acknowledged. A batching sink acknowledges a record only after it uploads the
batch that holds it, and it uploads an open batch when the batch fills, when its timer fires, or
when its input ends. A stopping sink with an open batch therefore waits for the batch's timer: a
stop takes up to the batch timeout, and a stop limit shorter than that kills Vector, which reads
the batch's records again at the next start.

The patch ends the input once every record in the buffer has been read, acknowledged or not: when
the writer is done and the buffer's size is no more than the bytes of the records the reader
returned and has not yet seen acknowledged. The writer publishes a record's size before it marks
itself done, so a record flushed just before the close is still read. Every sink flushes its open
batches at the end of its input, so a stop uploads them at once.

Acknowledgements reach the buffer's ledger only through its reader, so the sink task settles the
reader before it drops it: when the sink has finished normally, the task waits until every record
the reader returned is acknowledged and flushes the ledger. A sink that finished with an error is
not settled. It drops the requests it had in flight, and a dropped event counts as delivered, so
settling would record undelivered records as read; unsettled, they are read again at the next
start, as on upstream. A Vector killed between the end of a sink's input and its settle reads
delivered records again and loses none.

Tests: the five in `variants::disk_v2::tests::end_of_input` (`vector-buffers`), which end a reader
that holds unacknowledged records, read a record flushed just before the close, settle, and reopen
the buffer after a settled and an unsettled end; and
`topology_disk_buffer_ends_a_holding_sink_s_input_at_stop` (`vector`), which stops a topology whose
disk-buffered sink holds its events until its input ends, then reopens the buffer and reads
nothing.

## Building

[`eiti/build.sh <target>`](build.sh) builds one target (`x86_64-unknown-linux-gnu` or
`aarch64-unknown-linux-gnu`) on a Linux machine of that architecture. It first runs the tests
listed above, each by name, and stops when one fails or no longer exists; then it builds a release
binary with the component set the script names, checks that the binary reports the upstream
version and needs no glibc newer than upstream's floor, and packages it as upstream packages its
release: `vector-<version>-<target>.tar.gz`, holding `vector-<target>/bin/vector`.

The component set is smaller than upstream's release: the sources, transforms and sinks the hosts
that run this build configure (`FEATURES` in the script). A configuration naming any other
component fails `vector validate` on the binary.

## Publishing a build

A build is published by [`.github/workflows/eiti-release.yml`](../.github/workflows/eiti-release.yml),
which runs when a tag named `<version>-eiti.<n>` is pushed (`0.58.0-eiti.1`; `<n>` is one more than
the last build of that version). It runs `eiti/build.sh` for each target on a runner of that
architecture, and, when every target was built, publishes the tarballs with
`vector-<version>-SHA256SUMS`, the checksum of each, named and laid out as upstream's own checksum
file is.

```sh
git tag 0.58.0-eiti.1 && git push origin 0.58.0-eiti.1
```

Upstream's release workflow runs on tags that start with `v`, and this branch holds no workflow
but its own, so nothing else runs.

A release's files can be replaced by anyone who may write this repository's releases, and its
checksum file is the release's own statement. Whoever installs a build should record the checksums
at the time they adopt it, and verify every later download against that record.

## Moving to a new upstream version

1. Create `eiti/v<new version>` at upstream's tag `v<new version>`.
2. Cherry-pick the patch commits and the tooling commit from the previous branch. A patch that
   upstream has made unnecessary conflicts or comes out empty: leave it out, with its test, and
   remove its section above and its tests from `PATCH_TESTS` in `eiti/build.sh`.
3. Run `eiti/build.sh` for a target. A patch that applied and no longer does what it is for fails
   its test there.
4. Tag `<new version>-eiti.1` and push the tag.

When no patch is left, the branch has nothing to build: upstream's own release is the one to run.
