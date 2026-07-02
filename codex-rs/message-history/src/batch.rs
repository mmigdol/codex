use std::collections::VecDeque;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::path::Path;

use memchr::memchr_iter;

use super::HISTORY_READ_BUFFER_SIZE;
use super::HistoryConfig;
use super::HistoryEntry;
use super::MAX_RETRIES;
use super::RETRY_SLEEP;
use super::history_filepath;
use super::log_identity;

const MAX_BATCH_ROWS: usize = 128;
const MAX_BATCH_BYTES: usize = 64 * 1024;

/// Position of the newest record to include in a bounded history lookup.
///
/// The initial cursor identifies only an absolute row offset. Continuation cursors also retain a
/// byte position so older batches can scan backward from the previous batch instead of rescanning
/// the history prefix.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HistoryBatchCursor {
    end_offset: usize,
    byte_position: Option<u64>,
    observed_file_len: Option<u64>,
}

impl HistoryBatchCursor {
    /// Creates an initial cursor ending at the given absolute history offset.
    pub fn new(end_offset: usize) -> Self {
        Self {
            end_offset,
            byte_position: None,
            observed_file_len: None,
        }
    }

    /// Returns the absolute history offset covered first by this cursor.
    pub fn end_offset(self) -> usize {
        self.end_offset
    }

    /// Returns the byte position used to continue an older lookup without a prefix rescan.
    pub fn byte_position(self) -> Option<u64> {
        self.byte_position
    }

    fn anchored(end_offset: usize, byte_position: u64, observed_file_len: u64) -> Self {
        Self {
            end_offset,
            byte_position: Some(byte_position),
            observed_file_len: Some(observed_file_len),
        }
    }
}

/// One absolute history offset covered by a bounded lookup.
///
/// Malformed records retain their offset with `entry` set to `None`, allowing callers to continue
/// searching older valid records without changing offset semantics.
#[derive(Clone, Debug, PartialEq)]
pub struct HistoryBatchEntry {
    /// Zero-based position in the history file, counted from the oldest record.
    pub offset: usize,
    /// Parsed record, or `None` when the row at `offset` is malformed.
    pub entry: Option<HistoryEntry>,
}

/// A bounded newest-first suffix ending at a requested absolute history offset.
///
/// `next_older_cursor` identifies the next position a caller should request after exhausting
/// `entries`. It carries a byte position because the byte cap can make a batch contain fewer than
/// 128 rows and because continuation lookups must not rescan already traversed prefixes.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct HistoryBatch {
    /// Covered records in newest-to-oldest order.
    pub entries: Vec<HistoryBatchEntry>,
    /// Next position to request after exhausting `entries`.
    pub next_older_cursor: Option<HistoryBatchCursor>,
}

struct RawHistoryBatchEntry {
    offset: usize,
    byte_position: u64,
    bytes: Vec<u8>,
}

/// Look up a bounded batch of history records ending at `cursor`.
///
/// The file is opened, identity-checked, and shared-locked once. Records are counted from the
/// oldest offset on the initial lookup. Continuation lookups scan backward from the byte position
/// returned with the previous batch. The result retains at most 128 rows and 64 KiB of raw JSONL,
/// except that one oversized newest row is returned alone so callers always make progress.
pub fn lookup_batch(
    log_id: u64,
    cursor: HistoryBatchCursor,
    config: &HistoryConfig,
) -> HistoryBatch {
    let path = history_filepath(config);
    match lookup_batch_from_file(&path, log_id, cursor) {
        Ok(batch) => batch,
        Err(error) => {
            tracing::warn!(%error, "failed to read history batch");
            HistoryBatch {
                entries: Vec::new(),
                next_older_cursor: Some(cursor),
            }
        }
    }
}

fn lookup_batch_from_file(
    path: &Path,
    log_id: u64,
    cursor: HistoryBatchCursor,
) -> std::io::Result<HistoryBatch> {
    let mut file = OpenOptions::new().read(true).open(path)?;
    let current_log_id = log_identity(&file.metadata()?).unwrap_or(0);
    if log_id != 0 && current_log_id != log_id {
        return Ok(HistoryBatch::default());
    }

    for _ in 0..MAX_RETRIES {
        match file.try_lock_shared() {
            Ok(()) => return scan_batch(&mut file, cursor),
            Err(std::fs::TryLockError::WouldBlock) => std::thread::sleep(RETRY_SLEEP),
            Err(error) => return Err(error.into()),
        }
    }

    Err(std::io::Error::new(
        std::io::ErrorKind::WouldBlock,
        "could not acquire shared history lock after multiple attempts",
    ))
}

fn scan_batch(file: &mut File, cursor: HistoryBatchCursor) -> std::io::Result<HistoryBatch> {
    let file_len = file.metadata()?.len();
    if let (Some(byte_position), Some(observed_file_len)) =
        (cursor.byte_position, cursor.observed_file_len)
        && byte_position <= file_len
        && observed_file_len <= file_len
    {
        return scan_batch_backward(file, cursor.end_offset, byte_position, file_len);
    }

    file.seek(SeekFrom::Start(0))?;
    scan_batch_forward(file, cursor.end_offset, file_len)
}

fn scan_batch_forward(
    file: &mut File,
    end_offset: usize,
    file_len: u64,
) -> std::io::Result<HistoryBatch> {
    let mut suffix = VecDeque::new();
    let mut suffix_bytes = 0usize;
    let mut pending = Vec::new();
    let mut read_buffer = [0u8; HISTORY_READ_BUFFER_SIZE];
    let mut offset = 0usize;
    let mut byte_position = 0u64;

    loop {
        let read = file.read(&mut read_buffer)?;
        if read == 0 {
            if !pending.is_empty() && offset <= end_offset {
                retain_row(
                    &mut suffix,
                    &mut suffix_bytes,
                    offset,
                    byte_position,
                    pending,
                );
            }
            return Ok(finish_forward_batch(suffix, file_len));
        }

        let chunk = &read_buffer[..read];
        let chunk_start = file.stream_position()? - read as u64;
        let mut row_start = 0;
        for newline in memchr_iter(b'\n', chunk) {
            pending.extend_from_slice(&chunk[row_start..=newline]);
            if offset <= end_offset {
                retain_row(
                    &mut suffix,
                    &mut suffix_bytes,
                    offset,
                    byte_position,
                    std::mem::take(&mut pending),
                );
            }
            if offset == end_offset {
                return Ok(finish_forward_batch(suffix, file_len));
            }
            offset = offset.saturating_add(1);
            row_start = newline + 1;
            byte_position = chunk_start + row_start as u64;
        }
        pending.extend_from_slice(&chunk[row_start..]);
    }
}

fn scan_batch_backward(
    file: &mut File,
    end_offset: usize,
    end_byte_position: u64,
    file_len: u64,
) -> std::io::Result<HistoryBatch> {
    let mut entries = Vec::new();
    let mut entries_bytes = 0usize;
    let mut reversed_row = Vec::new();
    let mut read_buffer = [0u8; HISTORY_READ_BUFFER_SIZE];
    let mut read_end = end_byte_position;
    let mut offset = end_offset;

    while read_end > 0 {
        let read_start = read_end.saturating_sub(HISTORY_READ_BUFFER_SIZE as u64);
        let read_len = usize::try_from(read_end - read_start).unwrap_or(HISTORY_READ_BUFFER_SIZE);
        file.seek(SeekFrom::Start(read_start))?;
        file.read_exact(&mut read_buffer[..read_len])?;

        for index in (0..read_len).rev() {
            let byte = read_buffer[index];
            if byte == b'\n' && !reversed_row.is_empty() {
                reversed_row.reverse();
                let raw = RawHistoryBatchEntry {
                    offset,
                    byte_position: read_start + index as u64 + 1,
                    bytes: std::mem::take(&mut reversed_row),
                };
                if !retain_newest_row(&mut entries, &mut entries_bytes, raw) {
                    return Ok(finish_batch(entries, file_len));
                }
                let Some(next_offset) = offset.checked_sub(1) else {
                    return Ok(finish_batch(entries, file_len));
                };
                offset = next_offset;
                reversed_row.push(b'\n');
            } else {
                reversed_row.push(byte);
            }
        }
        read_end = read_start;
    }

    if !reversed_row.is_empty() {
        reversed_row.reverse();
        retain_newest_row(
            &mut entries,
            &mut entries_bytes,
            RawHistoryBatchEntry {
                offset,
                byte_position: 0,
                bytes: reversed_row,
            },
        );
    }
    Ok(finish_batch(entries, file_len))
}

fn retain_row(
    suffix: &mut VecDeque<RawHistoryBatchEntry>,
    suffix_bytes: &mut usize,
    offset: usize,
    byte_position: u64,
    bytes: Vec<u8>,
) {
    let row_bytes = bytes.len();
    if row_bytes > MAX_BATCH_BYTES {
        suffix.clear();
        *suffix_bytes = row_bytes;
        suffix.push_back(RawHistoryBatchEntry {
            offset,
            byte_position,
            bytes,
        });
        return;
    }

    *suffix_bytes += row_bytes;
    suffix.push_back(RawHistoryBatchEntry {
        offset,
        byte_position,
        bytes,
    });
    while suffix.len() > MAX_BATCH_ROWS || *suffix_bytes > MAX_BATCH_BYTES {
        if let Some(removed) = suffix.pop_front() {
            *suffix_bytes -= removed.bytes.len();
        }
    }
}

fn retain_newest_row(
    entries: &mut Vec<RawHistoryBatchEntry>,
    entries_bytes: &mut usize,
    entry: RawHistoryBatchEntry,
) -> bool {
    let row_bytes = entry.bytes.len();
    if entries.is_empty() && row_bytes > MAX_BATCH_BYTES {
        entries.push(entry);
        return false;
    }
    if entries.len() == MAX_BATCH_ROWS || entries_bytes.saturating_add(row_bytes) > MAX_BATCH_BYTES
    {
        return false;
    }
    *entries_bytes += row_bytes;
    entries.push(entry);
    true
}

fn finish_forward_batch(suffix: VecDeque<RawHistoryBatchEntry>, file_len: u64) -> HistoryBatch {
    finish_batch(suffix.into_iter().rev().collect(), file_len)
}

fn finish_batch(entries: Vec<RawHistoryBatchEntry>, file_len: u64) -> HistoryBatch {
    let next_older_cursor = entries.last().and_then(|entry| {
        entry.offset.checked_sub(1).map(|end_offset| {
            HistoryBatchCursor::anchored(end_offset, entry.byte_position, file_len)
        })
    });
    let entries = entries
        .into_iter()
        .map(|raw| HistoryBatchEntry {
            offset: raw.offset,
            entry: parse_entry(&raw.bytes),
        })
        .collect();
    HistoryBatch {
        entries,
        next_older_cursor,
    }
}

fn parse_entry(raw: &[u8]) -> Option<HistoryEntry> {
    let raw = raw.strip_suffix(b"\n").unwrap_or(raw);
    let raw = raw.strip_suffix(b"\r").unwrap_or(raw);
    serde_json::from_slice(raw).ok()
}
