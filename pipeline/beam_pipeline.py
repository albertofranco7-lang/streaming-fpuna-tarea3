import sys
import os
os.environ.setdefault('PYTHONUTF8', '1')
#!/usr/bin/env python3
"""
Pipeline Apache Beam — Streaming de Transacciones Bancarias
===========================================================
Alumno: Jorge Alberto Franco Mora, CI: 3840694

Características demostradas
---------------------------
1. Lectura desde Kafka (source)
2. Tiempo de evento (event time) via timestamps extraídos del payload
3. Ventanas fijas de 30 segundos (Fixed Windows)
4. Deduplicación por event_id dentro de cada ventana
5. Agregación: suma de montos y conteo por (account_id, ventana)
6. Salida idempotente a archivos JSON en ./output/
   (nombre de archivo incluye la clave de ventana → sin duplicados en disco)

Nota sobre runner
-----------------
Usamos DirectRunner (local) para poder ejecutar sin un cluster Flink/Dataflow.
El soporte nativo de Kafka en DirectRunner requiere el paquete
`apache-beam[interactive]` o el uso del conector de Kafka de Beam.

Para una demo offline (sin Kafka real) se incluye un modo --offline que
lee de un archivo JSONL local y aplica los mismos transforms.
"""

import argparse
import hashlib
import json
import os
import time
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam import window
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.trigger import (
    AccumulationMode,
    AfterProcessingTime,
    AfterWatermark,
    Repeatedly,
)

# ──────────────────────────────────────────────
# Constantes
# ──────────────────────────────────────────────
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC_NAME      = "transactions"
WINDOW_SIZE_SEC = 30          # ventana fija de 30 segundos
ALLOWED_LATENESS_SEC = 60     # aceptar eventos hasta 60 s tarde
OUTPUT_DIR      = os.path.join(os.path.dirname(__file__), "..", "output")


# ──────────────────────────────────────────────
# DoFns
# ──────────────────────────────────────────────

class ParseAndTimestamp(beam.DoFn):
    """Parsea el JSON y asigna el timestamp de evento."""

    def process(self, element, *args, **kwargs):
        try:
            if isinstance(element, bytes):
                element = element.decode("utf-8")
            if isinstance(element, tuple):
                # mensaje Kafka: (key_bytes, value_bytes)
                element = element[1].decode("utf-8") if isinstance(element[1], bytes) else element[1]
            record = json.loads(element)
            ts_str = record.get("event_time", "")
            ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            timestamp = ts.timestamp()
            yield beam.window.TimestampedValue(record, timestamp)
        except Exception as exc:
            print(f"[ParseAndTimestamp] Error parseando elemento: {exc} | raw={element!r}")


class DeduplicateByEventId(beam.DoFn):
    """
    Dentro de cada ventana, emite cada event_id una sola vez.
    Usa un set en memoria dentro del bundle (válido para DirectRunner).
    Para runners distribuidos, se usa beam.Distinct() o state API.
    """

    def start_bundle(self):
        self._seen: set[str] = set()

    def process(self, element, *args, **kwargs):
        event_id = element.get("event_id", "")
        if event_id not in self._seen:
            self._seen.add(event_id)
            yield element


class FormatWindowedResult(beam.DoFn):
    """Formatea el resultado de la ventana para salida JSON."""

    def process(self, element, window=beam.DoFn.WindowParam, *args, **kwargs):
        key, aggregated = element
        account_id = key
        win_start = datetime.fromtimestamp(float(window.start), tz=timezone.utc).isoformat()
        win_end   = datetime.fromtimestamp(float(window.end),   tz=timezone.utc).isoformat()
        result = {
            "window_start": win_start,
            "window_end":   win_end,
            "account_id":   account_id,
            "total_amount": round(aggregated["total_amount"], 2),
            "tx_count":     aggregated["tx_count"],
            "currencies":   list(aggregated["currencies"]),
        }
        yield result


# ──────────────────────────────────────────────
# Combiners
# ──────────────────────────────────────────────

class SumTransactionsCombiner(beam.CombineFn):
    """Agrega transacciones: suma montos, cuenta txs, recopila monedas."""

    def create_accumulator(self):
        return {"total_amount": 0.0, "tx_count": 0, "currencies": set()}

    def add_input(self, accumulator, element):
        accumulator["total_amount"] += element.get("amount", 0.0)
        accumulator["tx_count"]     += 1
        accumulator["currencies"].add(element.get("currency", "UNKNOWN"))
        return accumulator

    def merge_accumulators(self, accumulators):
        merged = self.create_accumulator()
        for acc in accumulators:
            merged["total_amount"] += acc["total_amount"]
            merged["tx_count"]     += acc["tx_count"]
            merged["currencies"]  |= acc["currencies"]
        return merged

    def extract_output(self, accumulator):
        # Convertir set a lista para serialización JSON
        return {
            "total_amount": accumulator["total_amount"],
            "tx_count":     accumulator["tx_count"],
            "currencies":   list(accumulator["currencies"]),
        }


# ──────────────────────────────────────────────
# Salida idempotente
# ──────────────────────────────────────────────

class WriteIdempotentJson(beam.DoFn):
    """
    Escribe cada resultado en un archivo JSON cuyo nombre deriva de
    (window_start, account_id) — si se re-ejecuta, sobreescribe el mismo
    archivo con los mismos datos → idempotente.
    """

    def __init__(self, output_dir: str):
        self.output_dir = output_dir

    def setup(self):
        os.makedirs(self.output_dir, exist_ok=True)

    def process(self, element, *args, **kwargs):
        key = f"{element['window_start']}_{element['account_id']}"
        # Hash corto del key para nombre de archivo seguro
        h = hashlib.md5(key.encode()).hexdigest()[:10]
        filename = os.path.join(self.output_dir, f"result_{h}.json")
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(element, f, ensure_ascii=False, indent=2)
        print(f"[Output] Escrito: {filename} -> {element}")
        yield element


# ──────────────────────────────────────────────
# Modo OFFLINE: lee JSONL local
# ──────────────────────────────────────────────

def run_offline(input_file: str, output_dir: str):
    """
    Procesa un archivo JSONL local (un JSON por línea).
    Útil para pruebas sin Kafka.
    """
    print(f"\n[Pipeline] Modo OFFLINE — leyendo: {input_file}")
    options = PipelineOptions(["--runner=DirectRunner"])

    with beam.Pipeline(options=options) as p:
        results = (
            p
            | "ReadFile"   >> beam.io.ReadFromText(input_file)
            | "Parse+TS"   >> beam.ParDo(ParseAndTimestamp())
            | "Window"     >> beam.WindowInto(
                                  window.FixedWindows(WINDOW_SIZE_SEC),
                                  trigger=AfterWatermark(
                                      late=Repeatedly(AfterProcessingTime(5))
                                  ),
                                  accumulation_mode=AccumulationMode.DISCARDING,
                                  allowed_lateness=ALLOWED_LATENESS_SEC,
                              )
            | "Dedup"      >> beam.ParDo(DeduplicateByEventId())
            | "KeyByAcct"  >> beam.Map(lambda r: (r["account_id"], r))
            | "Aggregate"  >> beam.CombinePerKey(SumTransactionsCombiner())
            | "Format"     >> beam.ParDo(FormatWindowedResult())
            | "WriteOut"   >> beam.ParDo(WriteIdempotentJson(output_dir))
        )

    print("[Pipeline] OFFLINE completado.")


# ──────────────────────────────────────────────
# Modo STREAMING: lee desde Kafka
# ──────────────────────────────────────────────

def run_streaming(output_dir: str):
    """
    Lee mensajes de Kafka y aplica el pipeline de streaming.
    Requiere apache-beam con soporte Kafka (DirectRunner experimental).
    """
    print(f"\n[Pipeline] Modo STREAMING — conectando a {KAFKA_BOOTSTRAP}")

    # Importación condicional del conector Kafka de Beam
    try:
        from apache_beam.io.kafka import ReadFromKafka
    except ImportError:
        print("[ERROR] apache_beam.io.kafka no disponible.")
        print("        Instala: pip install apache-beam[gcp] o usa modo --offline")
        return

    options = PipelineOptions(
        runner="DirectRunner",
        streaming=True,
    )
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        (
            p
            | "ReadKafka" >> ReadFromKafka(
                consumer_config={
                    "bootstrap.servers": KAFKA_BOOTSTRAP,
                    "group.id":          "beam-streaming-group",
                    "auto.offset.reset": "earliest",
                },
                topics=[TOPIC_NAME],
                with_metadata=False,
            )
            | "Parse+TS"  >> beam.ParDo(ParseAndTimestamp())
            | "Window"    >> beam.WindowInto(
                                 window.FixedWindows(WINDOW_SIZE_SEC),
                                 trigger=AfterWatermark(
                                     late=Repeatedly(AfterProcessingTime(5))
                                 ),
                                 accumulation_mode=AccumulationMode.DISCARDING,
                                 allowed_lateness=ALLOWED_LATENESS_SEC,
                             )
            | "Dedup"     >> beam.ParDo(DeduplicateByEventId())
            | "KeyByAcct" >> beam.Map(lambda r: (r["account_id"], r))
            | "Aggregate" >> beam.CombinePerKey(SumTransactionsCombiner())
            | "Format"    >> beam.ParDo(FormatWindowedResult())
            | "WriteOut"  >> beam.ParDo(WriteIdempotentJson(output_dir))
        )


# ──────────────────────────────────────────────
# Entrypoint
# ──────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Beam Streaming Pipeline")
    parser.add_argument("--offline", action="store_true",
                        help="Modo offline: lee desde --input en vez de Kafka")
    parser.add_argument("--input",   default="../output/test_events.jsonl",
                        help="Archivo JSONL de entrada (solo modo --offline)")
    parser.add_argument("--output",  default=OUTPUT_DIR,
                        help="Directorio de salida")
    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)

    if args.offline:
        run_offline(args.input, args.output)
    else:
        run_streaming(args.output)
