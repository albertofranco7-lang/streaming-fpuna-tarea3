#!/usr/bin/env python3
"""
Productor de eventos sintéticos de transacciones bancarias.

Genera tres tipos de eventos especiales para demostrar el pipeline:
  - Eventos normales (en orden, sin duplicados)
  - Eventos duplicados (mismo event_id, enviado dos veces)
  - Eventos tardíos / fuera de orden (timestamp anterior a eventos ya enviados)

Formato del mensaje (JSON):
{
    "event_id":    "uuid4 único por evento de negocio",
    "account_id":  "ACC_001" … "ACC_010",
    "amount":      float,
    "currency":    "PYG",
    "event_time":  "ISO-8601 UTC",   # tiempo de evento (business time)
    "send_time":   "ISO-8601 UTC",   # tiempo de envío real
    "event_type":  "normal" | "duplicate" | "late"
}
"""

import json
import random
import time
import uuid
from datetime import datetime, timezone, timedelta

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ──────────────────────────────────────────────
# Configuración
# ──────────────────────────────────────────────
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC_NAME      = "transactions"
NUM_EVENTS      = 100          # eventos normales por ejecución
DUPLICATE_RATE  = 0.15         # 15 % de eventos serán duplicados
LATE_RATE       = 0.10         # 10 % de eventos serán tardíos
LATE_DELAY_SEC  = 45           # segundos de retraso para eventos tardíos

ACCOUNTS  = [f"ACC_{i:03d}" for i in range(1, 11)]
CURRENCIES = ["PYG", "USD", "BRL"]


def build_event(event_id: str, account_id: str, amount: float,
                currency: str, event_time: datetime,
                event_type: str = "normal") -> dict:
    """Construye el diccionario del evento."""
    return {
        "event_id":   event_id,
        "account_id": account_id,
        "amount":     round(amount, 2),
        "currency":   currency,
        "event_time": event_time.isoformat(),
        "send_time":  datetime.now(timezone.utc).isoformat(),
        "event_type": event_type,
    }


def connect_producer(retries: int = 10, delay: int = 5) -> KafkaProducer:
    """Intenta conectar al broker con reintentos."""
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                acks="all",            # espera confirmación de todos los ISR
                retries=3,
            )
            print(f"[Producer] Conectado a Kafka en {KAFKA_BOOTSTRAP}")
            return producer
        except NoBrokersAvailable:
            print(f"[Producer] Intento {attempt}/{retries}: broker no disponible, "
                  f"reintentando en {delay}s…")
            time.sleep(delay)
    raise RuntimeError("No se pudo conectar a Kafka después de varios intentos.")


def produce_events():
    producer = connect_producer()
    sent = 0
    duplicates_sent = 0
    late_sent = 0

    # Lista para guardar eventos candidatos a duplicado/tardío
    recent_events: list[dict] = []

    base_time = datetime.now(timezone.utc)

    print(f"[Producer] Enviando {NUM_EVENTS} eventos al tópico '{TOPIC_NAME}'…\n")

    for i in range(NUM_EVENTS):
        # Tiempo de evento avanza ~1 segundo por evento
        event_time = base_time + timedelta(seconds=i)
        event_id   = str(uuid.uuid4())
        account_id = random.choice(ACCOUNTS)
        amount     = random.uniform(1_000, 5_000_000)
        currency   = random.choice(CURRENCIES)

        # Evento normal
        event = build_event(event_id, account_id, amount, currency,
                            event_time, "normal")
        producer.send(TOPIC_NAME, key=event_id, value=event)
        recent_events.append(event)
        sent += 1

        # ── Evento duplicado ──────────────────────────────────
        if random.random() < DUPLICATE_RATE:
            dup = build_event(event_id, account_id, amount, currency,
                              event_time, "duplicate")
            # Pequeño retraso para simular re-delivery de red
            time.sleep(0.05)
            producer.send(TOPIC_NAME, key=event_id, value=dup)
            duplicates_sent += 1
            print(f"  [DUP]  event_id={event_id[:8]}… cuenta={account_id}")

        # ── Evento tardío (fuera de orden) ────────────────────
        if random.random() < LATE_RATE and len(recent_events) > 5:
            old_event  = random.choice(recent_events[:-5])
            late_time  = (
                datetime.fromisoformat(old_event["event_time"])
                - timedelta(seconds=LATE_DELAY_SEC)
            )
            late = build_event(
                old_event["event_id"],   # mismo id → deduplicado por el pipeline
                old_event["account_id"],
                old_event["amount"],
                old_event["currency"],
                late_time,
                "late",
            )
            producer.send(TOPIC_NAME, key=late["event_id"], value=late)
            late_sent += 1
            print(f"  [LATE] event_id={late['event_id'][:8]}… "
                  f"event_time={late_time.strftime('%H:%M:%S')}")

        # Flush cada 10 eventos y pequeño delay
        if i % 10 == 0:
            producer.flush()
            print(f"  [{i+1:03d}/{NUM_EVENTS}] Enviados hasta ahora…")
        time.sleep(0.1)

    producer.flush()
    producer.close()

    print("\n" + "="*50)
    print(f"[Producer] RESUMEN")
    print(f"  Eventos normales  : {sent}")
    print(f"  Duplicados enviados: {duplicates_sent}")
    print(f"  Tardíos enviados  : {late_sent}")
    print(f"  Total mensajes    : {sent + duplicates_sent + late_sent}")
    print("="*50)


if __name__ == "__main__":
    produce_events()
