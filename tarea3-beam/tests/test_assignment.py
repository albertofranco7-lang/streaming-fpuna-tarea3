from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms import trigger

DATA_PATH = Path(__file__).parents[1] / "data" / "payments.jsonl"


def load_events() -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in DATA_PATH.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def total_for(
    totals: list[dict[str, Any]],
    merchant_id: str,
    window_start: str,
) -> int:
    matches = [
        row["total"]
        for row in totals
        if row["merchant_id"] == merchant_id
        and row["window_start"] == window_start
    ]
    assert len(matches) == 1
    return matches[0]


def test_parse_utc_returns_timezone_aware_datetime(solution):
    parsed = solution.parse_utc("2026-07-24T13:00:05Z")

    assert parsed == datetime(2026, 7, 24, 13, 0, 5, tzinfo=UTC)
    assert parsed.utcoffset() is not None


def test_assign_fixed_window_uses_event_time(solution):
    timestamp = datetime(2026, 7, 24, 13, 0, 42, tzinfo=UTC)

    start, end = solution.assign_fixed_window(timestamp, 60)

    assert start == datetime(2026, 7, 24, 13, 0, tzinfo=UTC)
    assert end == datetime(2026, 7, 24, 13, 1, tzinfo=UTC)


def test_duplicate_does_not_change_total(solution):
    totals, audit = solution.summarize_payments(load_events())

    assert total_for(
        totals,
        "m-verde",
        "2026-07-24T13:00:00+00:00",
    ) == 80_000
    duplicate = next(row for row in audit if row["event_id"] == "p-002" and row["duplicate"])
    assert duplicate["accepted"] is False
    assert duplicate["reason"] == "duplicate"


def test_deduplication_is_isolated_by_merchant(solution):
    shared_id_events = [
        {
            "event_id": "shared",
            "merchant_id": "m-a",
            "event_time": "2026-07-24T13:00:05Z",
            "arrival_time": "2026-07-24T13:00:06Z",
            "amount": 10,
            "status": "CONFIRMED",
        },
        {
            "event_id": "shared",
            "merchant_id": "m-b",
            "event_time": "2026-07-24T13:00:10Z",
            "arrival_time": "2026-07-24T13:00:11Z",
            "amount": 20,
            "status": "CONFIRMED",
        },
    ]

    totals, _ = solution.summarize_payments(shared_id_events)

    assert {row["merchant_id"]: row["total"] for row in totals} == {
        "m-a": 10,
        "m-b": 20,
    }


def test_stateful_dofn_keeps_keys_isolated(solution):
    events = [
        ("m-a", {"event_id": "shared"}),
        ("m-b", {"event_id": "shared"}),
    ]

    with BeamTestPipeline() as pipeline:
        output = (
            pipeline
            | beam.Create(events)
            | beam.WindowInto(beam.window.FixedWindows(60))
            | beam.ParDo(solution.DeduplicatePayments())
            | beam.Map(lambda item: (item[0], item[1]["event_id"]))
        )
        assert_that(
            output,
            equal_to([("m-a", "shared"), ("m-b", "shared")]),
        )


def test_out_of_order_event_uses_its_event_time_window(solution):
    totals, _ = solution.summarize_payments(load_events())

    assert total_for(
        totals,
        "m-azul",
        "2026-07-24T13:00:00+00:00",
    ) == 170_000


def test_late_event_within_tolerance_is_a_revision(solution):
    _, audit = solution.summarize_payments(
        load_events(),
        allowed_lateness_seconds=180,
    )

    late = next(row for row in audit if row["event_id"] == "p-007")
    assert late["accepted"] is True
    assert late["revision"] is True
    assert late["reason"] == "accepted"


def test_event_beyond_lateness_is_audited(solution):
    _, audit = solution.summarize_payments(
        load_events(),
        allowed_lateness_seconds=120,
    )

    too_late = next(row for row in audit if row["event_id"] == "p-007")
    assert too_late["accepted"] is False
    assert too_late["too_late"] is True
    assert too_late["reason"] == "too_late"


def test_windowed_pipeline_produces_totals(solution):
    events = [
        {
            "event_id": "p-1",
            "merchant_id": "m-a",
            "event_time": "2026-07-24T13:00:05Z",
            "arrival_time": "2026-07-24T13:00:06Z",
            "amount": 10,
            "status": "CONFIRMED",
        },
        {
            "event_id": "p-2",
            "merchant_id": "m-a",
            "event_time": "2026-07-24T13:00:42Z",
            "arrival_time": "2026-07-24T13:00:43Z",
            "amount": 20,
            "status": "CONFIRMED",
        },
    ]

    with BeamTestPipeline() as pipeline:
        output = solution.build_windowed_totals_pipeline(
            pipeline,
            events,
            window_seconds=60,
        )
        assert_that(
            output,
            equal_to(
                [
                    {
                        "merchant_id": "m-a",
                        "window_start": "2026-07-24T13:00:00+00:00",
                        "window_end": "2026-07-24T13:01:00+00:00",
                        "total": 30,
                    }
                ]
            ),
        )


def test_trigger_policy_has_lateness_and_accumulating_panes(solution):
    policy = solution.build_trigger_policy(
        window_seconds=60,
        allowed_lateness_seconds=120,
    )

    assert isinstance(policy, beam.WindowInto)
    assert policy.windowing.windowfn.size.seconds == 60
    assert policy.windowing.allowed_lateness.seconds == 120
    assert policy.windowing.accumulation_mode == trigger.AccumulationMode.ACCUMULATING
    assert isinstance(policy.windowing.triggerfn, trigger.AfterWatermark)


def test_retries_converge_to_one_materialized_entity(solution):
    results = [
        {
            "merchant_id": "m-a",
            "window_start": "2026-07-24T13:00:00+00:00",
            "window_end": "2026-07-24T13:01:00+00:00",
            "total": 30,
        }
    ]

    materialized, audit = solution.simulate_sink_retries(
        results,
        attempts=2,
        idempotent=True,
    )

    assert len(audit) == 2
    assert len(materialized) == 1
    assert materialized[0]["idempotency_key"] == (
        "m-a|2026-07-24T13:00:00+00:00"
    )
    assert [row["attempt"] for row in audit] == [1, 2]
    assert all(row["operation"] == "UPSERT" for row in audit)


def test_append_only_sink_materializes_every_attempt(solution):
    results = [
        {
            "merchant_id": "m-a",
            "window_start": "2026-07-24T13:00:00+00:00",
            "window_end": "2026-07-24T13:01:00+00:00",
            "total": 30,
        }
    ]

    materialized, audit = solution.simulate_sink_retries(
        results,
        attempts=2,
        idempotent=False,
    )

    assert len(audit) == 2
    assert len(materialized) == 2
    assert all(row["operation"] == "POST" for row in audit)


def test_timer_handler_clears_state(solution):
    class FakeState:
        def __init__(self):
            self.clear_called = False

        def clear(self):
            self.clear_called = True

    fake_state = FakeState()

    solution.DeduplicatePayments().expire(seen_ids=fake_state)

    assert fake_state.clear_called is True
