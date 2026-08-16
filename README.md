# Pipeline de Streaming End-to-End con Apache Kafka y Apache Beam

**Alumno:** Jorge Alberto Franco Mora  
**CI:** 3840694  
**Maestría:** Inteligencia Artificial y Análisis de Datos  
**Facultad:** Facultad Politécnica — Universidad Nacional de Asunción  
**Módulo:** 14 — Streaming de Datos y sus Aplicaciones

---

## Descripción del Proyecto

Pipeline de streaming end-to-end que procesa **transacciones bancarias sintéticas**.
Demuestra los conceptos fundamentales de streaming de datos:

| Concepto | Implementación |
|---|---|
| Fuente de datos | Productor Python → Apache Kafka |
| Tiempo de evento | Timestamp extraído del payload (`event_time`) |
| Ventanas | Ventanas fijas de 30 segundos (Fixed Windows) |
| Deduplicación | Por `event_id` dentro de cada ventana |
| Salida idempotente | Archivos JSON con nombre derivado de (ventana, cuenta) |
| Eventos duplicados | 15% de eventos re-enviados con mismo `event_id` |
| Eventos tardíos | 10% de eventos con `event_time` 45s en el pasado |

---

## Arquitectura

```
┌──────────────────────────────────────────────────────────────┐
│                    FUENTE DE DATOS                           │
│  producer/producer.py                                        │
│  • Genera eventos sintéticos de transacciones bancarias      │
│  • Inyecta duplicados (15%) y eventos tardíos (10%)          │
└────────────────────────┬─────────────────────────────────────┘
                         │ JSON vía kafka-python
                         ▼
┌──────────────────────────────────────────────────────────────┐
│                    APACHE KAFKA                              │
│  Docker: confluentinc/cp-kafka:7.6.1                        │
│  Tópico: transactions (1 partición, replicación 1)           │
│  UI web: http://localhost:8080                               │
└────────────────────────┬─────────────────────────────────────┘
                         │ Consumer Group: beam-streaming-group
                         ▼
┌──────────────────────────────────────────────────────────────┐
│                 APACHE BEAM PIPELINE                         │
│  pipeline/beam_pipeline.py  (DirectRunner)                  │
│                                                              │
│  ReadFromKafka                                               │
│       │                                                      │
│  ParseAndTimestamp   ← asigna event_time como timestamp      │
│       │                                                      │
│  WindowInto(FixedWindows(30s))                               │
│    trigger: AfterWatermark + late Repeatedly(5s)             │
│    allowed_lateness: 60s                                     │
│       │                                                      │
│  DeduplicateByEventId  ← elimina duplicados por event_id     │
│       │                                                      │
│  Map(account_id → record)                                    │
│       │                                                      │
│  CombinePerKey(SumTransactionsCombiner)                      │
│    • total_amount: suma de montos                            │
│    • tx_count: conteo de transacciones                       │
│    • currencies: set de monedas                              │
│       │                                                      │
│  FormatWindowedResult  ← añade metadatos de ventana          │
│       │                                                      │
│  WriteIdempotentJson   ← nombre = hash(ventana + cuenta)     │
└────────────────────────┬─────────────────────────────────────┘
                         │ Archivos JSON
                         ▼
┌──────────────────────────────────────────────────────────────┐
│                       SALIDA                                 │
│  output/result_<hash>.json                                   │
│  • Idempotente: re-ejecución produce mismos archivos         │
│  • Un archivo por (ventana, cuenta)                          │
└──────────────────────────────────────────────────────────────┘
```

---

## Requisitos

- **Docker Desktop** (Windows) — para Kafka y Zookeeper
- **Python 3.10+** — para el productor, pipeline y pruebas
- **pip** — para instalar dependencias

---

## Instalación paso a paso

### 1. Clonar / descomprimir el proyecto

```bash
cd C:\Users\<tu_usuario>\Desktop
# El proyecto ya está en streaming-pipeline/
```

### 2. Levantar Kafka con Docker

```bash
cd streaming-pipeline
docker compose up -d
```

Esperar ~30 segundos para que Kafka esté listo.  
Verificar en: **http://localhost:8080** (Kafka UI)

### 3. Instalar dependencias Python

```bash
# Productor
cd producer
pip install -r requirements.txt
cd ..

# Pipeline
cd pipeline
pip install -r requirements.txt
cd ..

# Pruebas
cd tests
pip install -r requirements.txt
cd ..
```

### 4. Generar datos de prueba offline

```bash
python tests/generate_test_data.py
# → crea output/test_events.jsonl
```

### 5. Ejecutar el pipeline en modo offline (sin Kafka)

```bash
python pipeline/beam_pipeline.py --offline --input output/test_events.jsonl
```

Los resultados aparecen en `output/result_*.json`.

### 6. Ejecutar pruebas unitarias

```bash
pytest tests/test_pipeline.py -v
```

### 7. Ejecutar end-to-end con Kafka real

En dos terminales separadas:

**Terminal 1 — Pipeline:**
```bash
python pipeline/beam_pipeline.py
```

**Terminal 2 — Productor:**
```bash
python producer/producer.py
```

---

## Estructura del proyecto

```
streaming-pipeline/
├── docker-compose.yml          # Kafka + Zookeeper + Kafka-UI
├── producer/
│   ├── producer.py             # Productor con duplicados y eventos tardíos
│   └── requirements.txt
├── pipeline/
│   ├── beam_pipeline.py        # Pipeline Beam: ventanas, dedup, agregación, salida
│   └── requirements.txt
├── tests/
│   ├── generate_test_data.py   # Generador de JSONL de prueba
│   ├── test_pipeline.py        # Pruebas unitarias (pytest)
│   └── requirements.txt
├── output/                     # Resultados JSON idempotentes
└── docs/
    └── arquitectura.md         # Documento técnico detallado
```

---

## Conceptos clave demostrados

### Tiempo de evento vs. tiempo de procesamiento
El campo `event_time` en el payload representa cuándo ocurrió la transacción
en el negocio. El pipeline lo usa como timestamp de Beam, no el tiempo de
llegada al broker.

### Ventanas fijas
`FixedWindows(30)` agrupa eventos en segmentos de 30 segundos basados en
`event_time`. Cada segmento produce resultados independientes.

### Watermark y latencia permitida
El trigger `AfterWatermark` dispara cuando Beam estima que todos los eventos
de una ventana han llegado. `allowed_lateness=60` acepta eventos hasta 60s
después del cierre de la ventana.

### Deduplicación
`DeduplicateByEventId` elimina eventos con `event_id` duplicado dentro de
cada ventana. Un evento re-enviado (network retry, productor fallido) no
afecta los agregados.

### Salida idempotente
El nombre de cada archivo de resultado es un hash de `(window_start, account_id)`.
Re-ejecutar el pipeline produce exactamente los mismos archivos con el mismo
contenido.

---

## Resultados de ejemplo

```json
{
  "window_start": "2026-08-16T10:00:00+00:00",
  "window_end":   "2026-08-16T10:00:30+00:00",
  "account_id":   "ACC_003",
  "total_amount": 4523819.45,
  "tx_count":     3,
  "currencies":   ["PYG", "USD"]
}
```

---

## Licencia

Proyecto académico — FPUNA, Maestría en IA y Análisis de Datos, 2026.
