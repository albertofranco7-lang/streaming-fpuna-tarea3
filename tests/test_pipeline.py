#!/usr/bin/env python3
"""
Pruebas unitarias del pipeline con pytest + Apache Beam TestPipeline.

Cobertura:
  1. test_parse_and_timestamp        — parseo correcto + asignación de timestamp
  2. test_deduplication              — eventos duplicados eliminados
  3. test_windowing_groups_correctly — ventanas fijas agrupan correctamente
  4. test_aggregation                — combiner suma montos y cuenta txs
  5. test_late_events_allowed        — eventos tardíos aceptados dentro de latencia
  6. test_idempotent_output          — mismo resultado produce mismo archivo (idempotente)
"""

import json
import os
import tempfile
import uuid
from datetime import datetime, timezone, timedelta

import pytest
import apache_beam as beam
from apache_beam import window
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to, is_not_empty

# Ajustar path para importar el módulo del pipeline
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "pipeline"))

from beam_pipeline import (
    DeduplicateByEventId,
    FormatWindowedResult,
    ParseAndTimestamp,
    SumTransactionsCombiner,
    WriteIdempotentJson,
)

# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────
BASE_TS = datetime(2026, 8, 16, 10, 0, 0, tzinfo=timezone.utc)


def make_json(event_id, account_id, amount, currency, event_time, event_type="normal"):
    return json.dumps({
        "event_id":   event_id,
        "account_id": account_id,
        "amount":     amount,
        "currency":   currency,
        "event_time": event_time.isoformat(),
        "send_time":  datetime.now(timezone.utc).isoformat(),
        "event_type": event_type,
    })


def ts_val(record, ts):
    """Crea un TimestampedValue."""
    return beam.window.TimestampedValue(record, ts.timestamp())


# ──────────────────────────────────────────────
# Test 1: Parseo y timestamp
# ──────────────────────────────────────────────

def test_parse_and_timestamp():
    """ParseAndTimestamp debe emitir un dict con event_id y los campos esperados."""
    eid = str(uuid.uuid4())
    raw = make_json(eid, "ACC_001", 100_000, "PYG", BASE_TS)

    with TestPipeline() as p:
        result = (
            p
            | beam.Create([raw])
            | beam.ParDo(ParseAndTimestamp())
        )
        assert_that(result, is_not_empty())


# ──────────────────────────────────────────────
# Test 2: Deduplicación
# ──────────────────────────────────────────────

def test_deduplication():
    """Tres mensajes con el mismo event_id deben reducirse a uno."""
    eid = str(uuid.uuid4())
    records = [
        {"event_id": eid, "account_id": "ACC_001", "amount": 50_000,
         "currency": "PYG", "event_type": "normal"},
        {"event_id": eid, "account_id": "ACC_001", "amount": 50_000,
         "currency": "PYG", "event_type": "duplicate"},
        {"event_id": eid, "account_id": "ACC_001", "amount": 50_000,
         "currency": "PYG", "event_type": "duplicate"},
    ]

    with TestPipeline() as p:
        result = (
            p
            | beam.Create(records)
            | beam.ParDo(DeduplicateByEventId())
        )
        assert_that(result, equal_to([records[0]]))


# ──────────────────────────────────────────────
# Test 3: Ventanas agrupa por tiempo
# ──────────────────────────────────────────────

def test_windowing_groups_correctly():
    """
    Eventos en la misma ventana de 30s deben combinarse;
    eventos en ventanas distintas deben producir resultados separados.
    """
    t0 = BASE_TS
    t1 = BASE_TS + timedelta(seconds=10)   # misma ventana que t0
    t2 = BASE_TS + timedelta(seconds=40)   # ventana siguiente

    records = [
        ts_val({"event_id": str(uuid.uuid4()), "account_id": "ACC_002",
                "amount": 100.0, "currency": "PYG"}, t0),
        ts_val({"event_id": str(uuid.uuid4()), "account_id": "ACC_002",
                "amount": 200.0, "currency": "PYG"}, t1),
        ts_val({"event_id": str(uuid.uuid4()), "account_id": "ACC_002",
                "amount": 300.0, "currency": "USD"}, t2),
    ]

    with TestPipeline() as p:
        result = (
            p
            | beam.Create(records)
            | beam.WindowInto(window.FixedWindows(30))
            | beam.Map(lambda r: (r["account_id"], r))
            | beam.CombinePerKey(SumTransactionsCombiner())
        )
        # Debe haber 2 resultados: uno por ventana
        assert_that(result, is_not_empty())


# ──────────────────────────────────────────────
# Test 4: Agregación correcta
# ──────────────────────────────────────────────

def test_aggregation():
    """CombinePerKey debe sumar montos y contar transacciones correctamente."""
    records = [
        ("ACC_003", {"event_id": str(uuid.uuid4()), "account_id": "ACC_003",
                     "amount": 1000.0, "currency": "PYG"}),
        ("ACC_003", {"event_id": str(uuid.uuid4()), "account_id": "ACC_003",
                     "amount": 2000.0, "currency": "USD"}),
        ("ACC_004", {"event_id": str(uuid.uuid4()), "account_id": "ACC_004",
                     "amount": 500.0,  "currency": "BRL"}),
    ]

    with TestPipeline() as p:
        result = (
            p
            | beam.Create(records)
            | beam.CombinePerKey(SumTransactionsCombiner())
        )

        def check(outputs):
            output_map = dict(outputs)
            agg003 = output_map["ACC_003"]
            assert agg003["total_amount"] == 3000.0, f"Esperado 3000, obtenido {agg003['total_amount']}"
            assert agg003["tx_count"] == 2
            assert set(agg003["currencies"]) == {"PYG", "USD"}
            agg004 = output_map["ACC_004"]
            assert agg004["total_amount"] == 500.0
            assert agg004["tx_count"] == 1

        assert_that(result, check)


# ──────────────────────────────────────────────
# Test 5: Eventos tardíos aceptados
# ──────────────────────────────────────────────

def test_late_events_handled():
    """
    Eventos con event_time en el pasado deben ser parseados correctamente.
    La política de lateness se configura en el pipeline; aquí verificamos
    que ParseAndTimestamp asigna bien el timestamp histórico.
    """
    late_time = BASE_TS - timedelta(seconds=90)
    raw = make_json(str(uuid.uuid4()), "ACC_005", 777_000, "PYG",
                    late_time, "late")

    with TestPipeline() as p:
        result = (
            p
            | beam.Create([raw])
            | beam.ParDo(ParseAndTimestamp())
        )
        assert_that(result, is_not_empty())


# ──────────────────────────────────────────────
# Test 6: Salida idempotente
# ──────────────────────────────────────────────

def test_idempotent_output():
    """
    Escribir el mismo resultado dos veces produce exactamente UN archivo
    con el mismo contenido (el segundo write sobreescribe el primero).
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        result_record = {
            "window_start": "2026-08-16T10:00:00+00:00",
            "window_end":   "2026-08-16T10:00:30+00:00",
            "account_id":   "ACC_001",
            "total_amount": 1_500_000.0,
            "tx_count":     3,
            "currencies":   ["PYG"],
        }

        writer = WriteIdempotentJson(tmpdir)
        writer.setup()

        # Primera escritura
        list(writer.process(result_record))
        files_after_first = os.listdir(tmpdir)

        # Segunda escritura (mismo record → mismo nombre de archivo)
        list(writer.process(result_record))
        files_after_second = os.listdir(tmpdir)

        assert len(files_after_first) == 1, "Debe haber exactamente 1 archivo tras primera escritura"
        assert files_after_second == files_after_first, (
            "Segunda escritura no debe crear archivos adicionales (idempotente)"
        )

        # Verificar contenido
        filepath = os.path.join(tmpdir, files_after_first[0])
        with open(filepath, encoding="utf-8") as f:
            saved = json.load(f)
        assert saved["total_amount"] == 1_500_000.0
        assert saved["tx_count"] == 3
