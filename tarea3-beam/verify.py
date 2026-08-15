"""Script de verificacion rapida de la logica del notebook."""
import json
from datetime import UTC, datetime, timedelta, timezone


def parse_utc(raw_value: str) -> datetime:
    normalized = raw_value.replace("Z", "+00:00")
    return datetime.fromisoformat(normalized)


def assign_fixed_window(timestamp: datetime, size_seconds: int = 60):
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    total_seconds = int((timestamp - epoch).total_seconds())
    ws = (total_seconds // size_seconds) * size_seconds
    start = epoch + timedelta(seconds=ws)
    end = start + timedelta(seconds=size_seconds)
    return start, end


def summarize_payments(events, window_seconds=60, allowed_lateness_seconds=120, deduplicate=True):
    def _parse(raw):
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))

    def _window(ts):
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        total_secs = int((ts - epoch).total_seconds())
        ws_secs = (total_secs // window_seconds) * window_seconds
        start = epoch + timedelta(seconds=ws_secs)
        end = start + timedelta(seconds=window_seconds)
        return start, end

    totals_map = {}
    seen = {}
    audit = []

    for event in events:
        event_id = event["event_id"]
        merchant_id = event["merchant_id"]
        status = event["status"]
        amount = event.get("amount", 0)
        event_time = _parse(event["event_time"])
        arrival_time = _parse(event["arrival_time"])
        delay_seconds = (arrival_time - event_time).total_seconds()
        window_start, window_end = _window(event_time)
        ws_str = window_start.isoformat()
        we_str = window_end.isoformat()
        seen.setdefault(merchant_id, set())
        is_duplicate = deduplicate and (event_id in seen[merchant_id])
        is_late = arrival_time >= window_end
        is_too_late = delay_seconds > allowed_lateness_seconds

        audit_row = {
            "event_id": event_id, "merchant_id": merchant_id,
            "delay_seconds": delay_seconds, "duplicate": is_duplicate,
            "too_late": is_too_late, "accepted": False, "revision": False, "reason": "",
        }

        if is_duplicate:
            audit_row["reason"] = "duplicate"
        elif status != "CONFIRMED":
            audit_row["reason"] = "not_confirmed"
        elif is_too_late:
            audit_row["reason"] = "too_late"
        else:
            audit_row["accepted"] = True
            if is_late:
                audit_row["revision"] = True
            audit_row["reason"] = "accepted"
            key = (merchant_id, ws_str)
            if key not in totals_map:
                totals_map[key] = {"merchant_id": merchant_id, "window_start": ws_str, "window_end": we_str, "total": 0}
            totals_map[key]["total"] += amount
            seen[merchant_id].add(event_id)

        audit.append(audit_row)

    return list(totals_map.values()), audit


def make_idempotency_key(result):
    return f"{result['merchant_id']}|{result['window_start']}"


def simulate_sink_retries(results, attempts=2, idempotent=True):
    append_sink = []
    upsert_sink = {}
    audit = []

    for attempt in range(1, attempts + 1):
        for result in results:
            key = make_idempotency_key(result)
            row = {
                **result, "idempotency_key": key,
                "attempt": attempt,
                "operation": "UPSERT" if idempotent else "POST",
            }
            audit.append(row)
            if idempotent:
                upsert_sink[key] = {**result, "idempotency_key": key}
            else:
                append_sink.append({**result, "idempotency_key": key})

    materialized = list(upsert_sink.values()) if idempotent else append_sink
    return materialized, audit


# ---- TESTS ----

def test_parse_utc():
    parsed = parse_utc("2026-07-24T13:00:05Z")
    assert parsed == datetime(2026, 7, 24, 13, 0, 5, tzinfo=UTC)
    assert parsed.utcoffset() is not None
    print("  test_parse_utc: PASS")


def test_assign_fixed_window():
    ts = datetime(2026, 7, 24, 13, 0, 42, tzinfo=UTC)
    start, end = assign_fixed_window(ts, 60)
    assert start == datetime(2026, 7, 24, 13, 0, tzinfo=UTC)
    assert end == datetime(2026, 7, 24, 13, 1, tzinfo=UTC)
    print("  test_assign_fixed_window: PASS")


def load_events():
    with open("data/payments.jsonl", encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def test_duplicate_does_not_change_total():
    events = load_events()
    totals, audit = summarize_payments(events)
    verde = next(r for r in totals if r["merchant_id"] == "m-verde" and r["window_start"] == "2026-07-24T13:00:00+00:00")
    assert verde["total"] == 80_000, f"verde total={verde['total']}"
    dup = next(r for r in audit if r["event_id"] == "p-002" and r["duplicate"])
    assert dup["accepted"] is False
    assert dup["reason"] == "duplicate"
    print("  test_duplicate_does_not_change_total: PASS")


def test_deduplication_isolated_by_merchant():
    shared_events = [
        {"event_id": "shared", "merchant_id": "m-a", "event_time": "2026-07-24T13:00:05Z",
         "arrival_time": "2026-07-24T13:00:06Z", "amount": 10, "status": "CONFIRMED"},
        {"event_id": "shared", "merchant_id": "m-b", "event_time": "2026-07-24T13:00:10Z",
         "arrival_time": "2026-07-24T13:00:11Z", "amount": 20, "status": "CONFIRMED"},
    ]
    totals, _ = summarize_payments(shared_events)
    result = {r["merchant_id"]: r["total"] for r in totals}
    assert result == {"m-a": 10, "m-b": 20}, f"result={result}"
    print("  test_deduplication_isolated_by_merchant: PASS")


def test_out_of_order_event():
    events = load_events()
    totals, _ = summarize_payments(events)
    azul = next(r for r in totals if r["merchant_id"] == "m-azul" and r["window_start"] == "2026-07-24T13:00:00+00:00")
    assert azul["total"] == 170_000, f"azul total={azul['total']}"
    print("  test_out_of_order_event: PASS")


def test_late_event_within_tolerance():
    events = load_events()
    _, audit = summarize_payments(events, allowed_lateness_seconds=180)
    late = next(r for r in audit if r["event_id"] == "p-007")
    assert late["accepted"] is True, f"accepted={late['accepted']}"
    assert late["revision"] is True, f"revision={late['revision']}"
    assert late["reason"] == "accepted"
    print("  test_late_event_within_tolerance: PASS")


def test_event_beyond_lateness():
    events = load_events()
    _, audit = summarize_payments(events, allowed_lateness_seconds=120)
    too_late = next(r for r in audit if r["event_id"] == "p-007")
    delay = too_late["delay_seconds"]
    assert too_late["accepted"] is False, f"accepted={too_late['accepted']}"
    assert too_late["too_late"] is True, f"too_late={too_late['too_late']}"
    assert too_late["reason"] == "too_late"
    print(f"  test_event_beyond_lateness: PASS (delay={delay}s)")


def test_retries_idempotent():
    results = [{"merchant_id": "m-a", "window_start": "2026-07-24T13:00:00+00:00",
                "window_end": "2026-07-24T13:01:00+00:00", "total": 30}]
    materialized, audit = simulate_sink_retries(results, attempts=2, idempotent=True)
    assert len(audit) == 2
    assert len(materialized) == 1
    assert materialized[0]["idempotency_key"] == "m-a|2026-07-24T13:00:00+00:00"
    assert [r["attempt"] for r in audit] == [1, 2]
    assert all(r["operation"] == "UPSERT" for r in audit)
    print("  test_retries_idempotent: PASS")


def test_retries_append_only():
    results = [{"merchant_id": "m-a", "window_start": "2026-07-24T13:00:00+00:00",
                "window_end": "2026-07-24T13:01:00+00:00", "total": 30}]
    materialized, audit = simulate_sink_retries(results, attempts=2, idempotent=False)
    assert len(audit) == 2
    assert len(materialized) == 2
    assert all(r["operation"] == "POST" for r in audit)
    print("  test_retries_append_only: PASS")


if __name__ == "__main__":
    print("Verificando logica Python pura...")
    test_parse_utc()
    test_assign_fixed_window()
    test_duplicate_does_not_change_total()
    test_deduplication_isolated_by_merchant()
    test_out_of_order_event()
    test_late_event_within_tolerance()
    test_event_beyond_lateness()
    test_retries_idempotent()
    test_retries_append_only()
    print("\nTodas las verificaciones: PASS")
