#!/usr/bin/env python3
"""
Genera un archivo JSONL de prueba con:
  - Eventos normales
  - Duplicados exactos (mismo event_id)
  - Eventos tardíos (event_time en el pasado)
  - Eventos fuera de orden

Uso:
    python tests/generate_test_data.py
    → crea output/test_events.jsonl
"""

import json
import os
import random
import uuid
from datetime import datetime, timezone, timedelta

OUTPUT_FILE = os.path.join(
    os.path.dirname(__file__), "..", "output", "test_events.jsonl"
)

ACCOUNTS  = [f"ACC_{i:03d}" for i in range(1, 6)]
CURRENCIES = ["PYG", "USD", "BRL"]
BASE_TIME  = datetime(2026, 8, 16, 10, 0, 0, tzinfo=timezone.utc)


def make_event(event_id, account_id, amount, currency, event_time, event_type="normal"):
    return {
        "event_id":   event_id,
        "account_id": account_id,
        "amount":     round(amount, 2),
        "currency":   currency,
        "event_time": event_time.isoformat(),
        "send_time":  datetime.now(timezone.utc).isoformat(),
        "event_type": event_type,
    }


def generate():
    events = []
    pool = []  # eventos normales para duplicar/retrasar

    # ── 50 eventos normales (ordenados) ──────────────────────
    for i in range(50):
        eid  = str(uuid.uuid4())
        acct = random.choice(ACCOUNTS)
        amt  = random.uniform(10_000, 2_000_000)
        cur  = random.choice(CURRENCIES)
        ts   = BASE_TIME + timedelta(seconds=i * 2)
        ev   = make_event(eid, acct, amt, cur, ts, "normal")
        events.append(ev)
        pool.append(ev)

    # ── 10 duplicados exactos ────────────────────────────────
    for _ in range(10):
        orig = random.choice(pool)
        dup  = make_event(
            orig["event_id"], orig["account_id"],
            orig["amount"],   orig["currency"],
            datetime.fromisoformat(orig["event_time"]),
            "duplicate",
        )
        events.append(dup)

    # ── 8 eventos tardíos (event_time 90 s en el pasado) ────
    for _ in range(8):
        orig     = random.choice(pool)
        late_ts  = datetime.fromisoformat(orig["event_time"]) - timedelta(seconds=90)
        late     = make_event(
            str(uuid.uuid4()),        # nuevo id → no deduplicado, sí tardío
            orig["account_id"],
            orig["amount"] * 0.5,
            orig["currency"],
            late_ts,
            "late",
        )
        events.append(late)

    # ── 5 fuera de orden (reordenamos aleatoriamente el final) ─
    random.shuffle(events[-15:])

    # Escribir JSONL
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        for ev in events:
            f.write(json.dumps(ev, ensure_ascii=False) + "\n")

    print(f"[TestData] {len(events)} eventos escritos en: {OUTPUT_FILE}")
    print(f"  Normales  : 50")
    print(f"  Duplicados: 10  (mismo event_id → dedup por pipeline)")
    print(f"  Tardíos   : 8   (event_time 90s antes del watermark)")
    print(f"  Fuera de orden: últimos 15 reordenados aleatoriamente")


if __name__ == "__main__":
    generate()
