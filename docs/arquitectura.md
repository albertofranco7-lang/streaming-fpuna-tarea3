# Documento Técnico: Pipeline de Streaming End-to-End

**Alumno:** Jorge Alberto Franco Mora | CI: 3840694  
**Institución:** Facultad Politécnica — Universidad Nacional de Asunción  
**Módulo:** 14 — Streaming de Datos y sus Aplicaciones  
**Fecha:** Agosto 2026

---

## 1. Introducción

Este documento describe el diseño e implementación de un pipeline de streaming
end-to-end que procesa eventos de transacciones bancarias sintéticas utilizando
**Apache Kafka** como broker de mensajes y **Apache Beam** (DirectRunner) como
motor de procesamiento.

El sistema demuestra los siguientes conceptos fundamentales del streaming:
- Procesamiento basado en tiempo de evento (*event time*)
- Ventanas fijas (*fixed windows*)
- Deduplicación de eventos
- Manejo de eventos tardíos y fuera de orden
- Salida idempotente

---

## 2. Arquitectura del Sistema

### 2.1 Diagrama de Componentes

```
╔══════════════════════════════════════════════════════════════════╗
║  CAPA DE GENERACIÓN                                              ║
║                                                                  ║
║  ┌─────────────────────────────────────────────────────┐        ║
║  │  producer/producer.py                               │        ║
║  │                                                     │        ║
║  │  • 100 eventos normales (100ms entre eventos)       │        ║
║  │  • 15% duplicados (mismo event_id, re-enviado)      │        ║
║  │  • 10% tardíos (event_time 45s en el pasado)        │        ║
║  │                                                     │        ║
║  │  Payload JSON:                                      │        ║
║  │  {event_id, account_id, amount, currency,           │        ║
║  │   event_time, send_time, event_type}                │        ║
║  └───────────────────────┬─────────────────────────────┘        ║
╚══════════════════════════╪═══════════════════════════════════════╝
                           │ kafka-python (acks="all")
                           │ key = event_id
                           ▼
╔══════════════════════════════════════════════════════════════════╗
║  CAPA DE TRANSPORTE — APACHE KAFKA                               ║
║                                                                  ║
║  ┌─────────────────────────────────────────────────────┐        ║
║  │  Tópico: transactions                               │        ║
║  │  Particiones: 1  |  Replicación: 1                  │        ║
║  │  Retención: 1 hora                                  │        ║
║  │                                                     │        ║
║  │  Docker: confluentinc/cp-kafka:7.6.1               │        ║
║  │  + confluentinc/cp-zookeeper:7.6.1                 │        ║
║  │  + Kafka UI: provectuslabs/kafka-ui                 │        ║
║  └───────────────────────┬─────────────────────────────┘        ║
╚══════════════════════════╪═══════════════════════════════════════╝
                           │ ReadFromKafka
                           │ group.id = beam-streaming-group
                           ▼
╔══════════════════════════════════════════════════════════════════╗
║  CAPA DE PROCESAMIENTO — APACHE BEAM (DirectRunner)              ║
║                                                                  ║
║  Paso 1: ParseAndTimestamp                                       ║
║    • Deserializa JSON                                            ║
║    • Extrae event_time → asigna timestamp de Beam               ║
║    • Descarta mensajes malformados (log de errores)             ║
║                                                                  ║
║  Paso 2: WindowInto(FixedWindows(30s))                           ║
║    • Ventanas fijas de 30 segundos sobre event_time             ║
║    • Trigger: AfterWatermark                                     ║
║    • Late trigger: Repeatedly(AfterProcessingTime(5s))          ║
║    • allowed_lateness = 60 segundos                             ║
║    • AccumulationMode: DISCARDING                               ║
║                                                                  ║
║  Paso 3: DeduplicateByEventId                                    ║
║    • Set de event_ids vistos por bundle                         ║
║    • Elimina re-entregas dentro de la ventana                   ║
║                                                                  ║
║  Paso 4: Map(account_id → record)                               ║
║    • Extrae clave de agrupación                                 ║
║                                                                  ║
║  Paso 5: CombinePerKey(SumTransactionsCombiner)                 ║
║    • total_amount: suma acumulada de montos                     ║
║    • tx_count: contador de transacciones únicas                 ║
║    • currencies: conjunto de monedas presentes                  ║
║                                                                  ║
║  Paso 6: FormatWindowedResult                                    ║
║    • Agrega metadatos: window_start, window_end                 ║
║                                                                  ║
║  Paso 7: WriteIdempotentJson                                     ║
║    • filename = hash_md5(window_start + account_id)[:10]        ║
║    • Sobreescritura determinista → idempotente                  ║
╚══════════════════════════╪═══════════════════════════════════════╝
                           │ JSON files
                           ▼
╔══════════════════════════════════════════════════════════════════╗
║  CAPA DE ALMACENAMIENTO — output/                                ║
║                                                                  ║
║  output/result_<hash>.json                                       ║
║  Ejemplo:                                                        ║
║  {                                                               ║
║    "window_start": "2026-08-16T10:00:00+00:00",                 ║
║    "window_end":   "2026-08-16T10:00:30+00:00",                 ║
║    "account_id":   "ACC_001",                                    ║
║    "total_amount": 4523819.45,                                   ║
║    "tx_count":     3,                                            ║
║    "currencies":   ["PYG", "USD"]                               ║
║  }                                                               ║
╚══════════════════════════════════════════════════════════════════╝
```

---

## 3. Modelo de Datos

### Evento de entrada (Kafka)

| Campo | Tipo | Descripción |
|---|---|---|
| `event_id` | UUID4 | Identificador único de negocio |
| `account_id` | string | Cuenta bancaria (ACC_001…ACC_010) |
| `amount` | float | Monto de la transacción |
| `currency` | string | Moneda (PYG, USD, BRL) |
| `event_time` | ISO-8601 UTC | Tiempo de evento (business time) |
| `send_time` | ISO-8601 UTC | Tiempo de envío real al broker |
| `event_type` | enum | "normal", "duplicate", "late" |

### Resultado de salida (JSON)

| Campo | Tipo | Descripción |
|---|---|---|
| `window_start` | ISO-8601 UTC | Inicio de la ventana |
| `window_end` | ISO-8601 UTC | Fin de la ventana |
| `account_id` | string | Cuenta bancaria |
| `total_amount` | float | Suma de montos deduplicados |
| `tx_count` | int | Conteo de transacciones únicas |
| `currencies` | list[string] | Monedas presentes en la ventana |

---

## 4. Decisiones de Diseño

### 4.1 Tiempo de evento vs. tiempo de procesamiento

Se usa el campo `event_time` del payload como timestamp de Beam.
Esto garantiza que eventos tardíos (llegados después de haber sido generados)
sean asignados a la ventana correcta de negocio, no a la ventana del momento
de llegada.

### 4.2 Ventanas fijas de 30 segundos

Permiten agregar transacciones por cuenta en intervalos pequeños, demostrando
la semántica de ventanas sobre tiempo de evento. En producción, el tamaño
dependería del SLA de reporting (ej: 5 minutos para alertas de fraude).

### 4.3 Latencia permitida de 60 segundos

`allowed_lateness=60s` acepta eventos que lleguen hasta 1 minuto después del
cierre de la ventana. El trigger tardío `Repeatedly(AfterProcessingTime(5s))`
re-dispara cada 5 segundos mientras arriben eventos tardíos.

### 4.4 Deduplicación por event_id

El productor puede re-enviar un evento (simulando fallos de red o reintentos).
El pipeline filtra duplicados usando un `set` de `event_id` por bundle.
Para producción distribuida, se reemplaza con `beam.Distinct()` o la
State API de Beam.

### 4.5 Salida idempotente

El nombre del archivo de salida es `result_<md5(window_start+account_id)[:10]>.json`.
Al re-ejecutar el pipeline con los mismos datos, se produce exactamente el mismo
nombre de archivo con el mismo contenido. No se acumulan resultados duplicados.

### 4.6 DirectRunner

Se usa el DirectRunner local para facilitar la demostración sin infraestructura
adicional. El código es compatible con Apache Flink o Google Cloud Dataflow
cambiando únicamente el runner en `PipelineOptions`.

---

## 5. Pruebas

### 5.1 Pruebas unitarias (pytest)

| Test | Qué verifica |
|---|---|
| `test_parse_and_timestamp` | Parseo correcto de JSON y asignación de timestamp |
| `test_deduplication` | 3 mensajes con mismo event_id → 1 resultado |
| `test_windowing_groups_correctly` | Eventos en ventanas distintas agrupados separadamente |
| `test_aggregation` | Suma de montos y conteo correctos |
| `test_late_events_handled` | Eventos con event_time pasado parseados correctamente |
| `test_idempotent_output` | Mismo resultado → mismo archivo (no duplicación) |

### 5.2 Prueba end-to-end

1. Levantar Kafka con `docker compose up -d`
2. Ejecutar `python tests/generate_test_data.py` → genera 73 eventos (50 normales + 10 dup + 8 tardíos + reorden)
3. Ejecutar `python pipeline/beam_pipeline.py --offline` → procesa el JSONL
4. Verificar archivos en `output/` → un JSON por (ventana, cuenta)
5. Re-ejecutar el paso 3 → mismos archivos, mismo contenido (idempotencia confirmada)

---

## 6. Instrucciones de Ejecución

Ver **README.md** en la raíz del proyecto.

---

## 7. Referencias

- Apache Beam Python SDK: https://beam.apache.org/documentation/sdks/python/
- Apache Kafka: https://kafka.apache.org/documentation/
- Confluent Platform Docker Images: https://hub.docker.com/u/confluentinc
- The Dataflow Model (Akidau et al., 2015): https://research.google/pubs/the-dataflow-model/
- Streaming Systems (Tyler Akidau, Slava Chernyak, Reuven Lax) — O'Reilly, 2018
