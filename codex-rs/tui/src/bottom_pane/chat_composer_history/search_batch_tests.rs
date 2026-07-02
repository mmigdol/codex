use pretty_assertions::assert_eq;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::unbounded_channel;

use super::*;
use crate::app_event::AppEvent;
use crate::app_event::HistoryBatchEntryResponse;

fn thread_id(value: u8) -> ThreadId {
    ThreadId::from_string(&format!("00000000-0000-0000-0000-{value:012}"))
        .expect("thread id should parse")
}

fn batch_entry(offset: usize, entry: Option<&str>) -> HistoryBatchEntryResponse {
    HistoryBatchEntryResponse {
        offset,
        entry: entry.map(str::to_string),
    }
}

fn history(
    entry_count: usize,
) -> (
    ChatComposerHistory,
    AppEventSender,
    UnboundedReceiver<AppEvent>,
) {
    let (tx, rx) = unbounded_channel();
    let tx = AppEventSender::new(tx);
    let mut history = ChatComposerHistory::new();
    history.set_metadata(thread_id(1), /*log_id*/ 42, entry_count);
    (history, tx, rx)
}

fn start_older_search(
    history: &mut ChatComposerHistory,
    tx: &AppEventSender,
    rx: &mut UnboundedReceiver<AppEvent>,
    query: &str,
    newest_offset: usize,
) -> HistoryBatchCursor {
    assert_eq!(
        history.search(
            query,
            HistorySearchDirection::Older,
            /*restart*/ true,
            tx
        ),
        HistorySearchResult::Pending
    );
    let AppEvent::LookupMessageHistoryEntry { offset, log_id, .. } =
        rx.try_recv().expect("newest entry request")
    else {
        panic!("expected newest single-entry request");
    };
    assert_eq!((offset, log_id), (newest_offset, 42));
    assert_eq!(
        history.on_entry_response(log_id, offset, Some("unrelated entry".to_string()), tx),
        HistoryEntryResponse::Search(HistorySearchResult::Pending)
    );
    let AppEvent::LookupMessageHistoryBatch { cursor, .. } =
        rx.try_recv().expect("older batch request")
    else {
        panic!("expected bounded batch request");
    };
    assert_eq!(cursor.end_offset(), newest_offset - 1);
    cursor
}

#[test]
fn search_batch_late_data_is_cache_only_after_cancel_or_query_edit() {
    let (mut cancelled, tx, mut rx) = history(5);
    start_older_search(&mut cancelled, &tx, &mut rx, "cached", 4);
    cancelled.reset_search();
    assert_eq!(
        cancelled.on_batch_response(
            42,
            HistoryBatchCursor::new(3),
            vec![batch_entry(3, Some("cached match"))],
            Some(HistoryBatchCursor::new(2)),
            &tx,
        ),
        None
    );
    assert_eq!(
        cancelled.search(
            "cached",
            HistorySearchDirection::Older,
            /*restart*/ true,
            &tx
        ),
        HistorySearchResult::Found(HistoryEntry::new("cached match".to_string()))
    );
    assert!(rx.try_recv().is_err());

    let (mut edited, tx, mut rx) = history(5);
    start_older_search(&mut edited, &tx, &mut rx, "old", 4);
    assert_eq!(
        edited.search(
            "new",
            HistorySearchDirection::Older,
            /*restart*/ true,
            &tx
        ),
        HistorySearchResult::Pending
    );
    let AppEvent::LookupMessageHistoryEntry { offset, .. } =
        rx.try_recv().expect("edited query request")
    else {
        panic!("expected edited-query single request");
    };
    assert_eq!(offset, 3);
    assert_eq!(
        edited.on_batch_response(
            42,
            HistoryBatchCursor::new(3),
            vec![batch_entry(3, Some("old data"))],
            Some(HistoryBatchCursor::new(2)),
            &tx,
        ),
        None
    );
    assert_eq!(
        edited.on_entry_response(42, 3, Some("new current match".to_string()), &tx),
        HistoryEntryResponse::Search(HistorySearchResult::Found(HistoryEntry::new(
            "new current match".to_string()
        )))
    );
}

#[test]
fn search_batch_rejects_stale_thread_and_log_metadata() {
    let (mut history, tx, mut rx) = history(5);
    start_older_search(&mut history, &tx, &mut rx, "stale", 4);
    history.set_metadata(thread_id(2), /*log_id*/ 43, /*entry_count*/ 5);
    assert_eq!(
        history.on_batch_response(
            42,
            HistoryBatchCursor::new(3),
            vec![batch_entry(3, Some("stale match"))],
            Some(HistoryBatchCursor::new(2)),
            &tx,
        ),
        None
    );
    assert!(history.fetched_history.is_empty());
    assert!(history.fetched_history_misses.is_empty());
}

#[test]
fn search_batch_read_failure_stops_after_bounded_retries() {
    let (mut history, tx, mut rx) = history(5);
    let cursor = start_older_search(&mut history, &tx, &mut rx, "retry", 4);

    for _ in 0..MAX_BATCH_READ_RETRIES {
        assert_eq!(
            history.on_batch_error(42, cursor, &tx),
            Some(HistorySearchResult::Pending)
        );
        let AppEvent::LookupMessageHistoryBatch {
            cursor: retry_cursor,
            log_id,
            ..
        } = rx.try_recv().expect("retry batch request")
        else {
            panic!("expected bounded batch retry");
        };
        assert_eq!((retry_cursor, log_id), (cursor, 42));
    }

    assert_eq!(
        history.on_batch_error(42, cursor, &tx),
        Some(HistorySearchResult::Unavailable)
    );
    assert!(rx.try_recv().is_err());
    assert_eq!(
        history.search.as_ref().and_then(|search| search.awaiting),
        None
    );
}

#[test]
fn search_batch_absent_1024_uses_one_single_and_eight_batches() {
    let (mut history, tx, mut rx) = history(1_024);
    let mut cursor = start_older_search(&mut history, &tx, &mut rx, "absent", 1_023);
    let mut batches = 0;

    loop {
        let end_offset = cursor.end_offset();
        let start_offset = end_offset.saturating_sub(127);
        let entries = (start_offset..=end_offset)
            .rev()
            .map(|offset| batch_entry(offset, Some("unrelated entry")))
            .collect();
        let next_older_cursor = start_offset.checked_sub(1).map(HistoryBatchCursor::new);
        let expected = if next_older_cursor.is_some() {
            HistorySearchResult::Pending
        } else {
            HistorySearchResult::NotFound
        };
        assert_eq!(
            history.on_batch_response(42, cursor, entries, next_older_cursor, &tx),
            Some(expected)
        );
        batches += 1;
        let Some(expected_cursor) = next_older_cursor else {
            break;
        };
        let AppEvent::LookupMessageHistoryBatch { cursor: next, .. } =
            rx.try_recv().expect("next older batch")
        else {
            panic!("expected bounded batch request");
        };
        assert_eq!(next, expected_cursor);
        cursor = next;
    }

    assert_eq!((1 + batches, batches), (9, 8));
    assert!(rx.try_recv().is_err());
}
