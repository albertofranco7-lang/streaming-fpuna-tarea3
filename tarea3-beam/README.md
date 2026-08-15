# Tarea 3 — Estado, duplicados e idempotencia con Apache Beam

**Alumno:** Jorge Alberto Franco Mora — CI 3840694  
**Asignatura:** Streaming de datos y sus aplicaciones  
**Maestría:** Inteligencia Artificial y Análisis de Datos  
**Facultad Politécnica — Universidad Nacional de Asunción**

---

## Objetivo

Producir totales confirmados por comercio y minuto usando Apache Beam con:

- `event_time` como timestamp del dominio (no arrival time).
- Ventanas fijas de 60 segundos.
- Tolerancia de hasta 120 segundos de lateness.
- Deduplicación por `event_id` aislada por comercio.
- Panes acumulativos.
- Sink idempotente mediante clave `merchant_id|window_start`.

---

## Ejecución rápida

### Con `uv` (recomendado)

```bash
# Instalar uv si no lo tenés: https://docs.astral.sh/uv/getting-started/installation/
uv sync --frozen
uv run pytest -v
```

Para abrir el notebook en modo editor:

```bash
uv run marimo edit notebook.py
```

### Con Docker

```bash
docker compose up --build notebook
```

Abrir [http://localhost:2718](http://localhost:2718).

Para correr los tests dentro del contenedor:

```bash
docker compose run --rm tests
```

### Validar estilo

```bash
uv run ruff check notebook.py
uv run marimo check --strict notebook.py
```

---

## Dataset (`data/payments.jsonl`)

| event_id | merchant_id | event_time | arrival_time | amount | status | Nota |
|----------|-------------|------------|--------------|--------|--------|------|
| p-001 | m-azul | 13:00:05 | 13:00:06 | 120 000 | CONFIRMED | — |
| p-002 | m-verde | 13:00:18 | 13:00:20 | 80 000 | CONFIRMED | — |
| p-003 | m-azul | 13:01:12 | 13:01:14 | 55 000 | PENDING | descartado |
| p-004 | m-azul | 13:00:42 | 13:01:35 | 50 000 | CONFIRMED | **fuera de orden** |
| p-002 | m-verde | 13:00:18 | 13:01:41 | 80 000 | CONFIRMED | **duplicado** |
| p-005 | m-verde | 13:01:44 | 13:01:46 | 90 000 | CONFIRMED | — |
| p-006 | m-azul | 13:02:02 | 13:02:03 | 200 000 | CONFIRMED | — |
| p-007 | m-verde | 13:00:51 | 13:03:40 | 30 000 | CONFIRMED | **muy tardío** (169 s) |
| p-008 | m-azul | 13:02:27 | 13:02:30 | 75 000 | REJECTED | descartado |

Con `allowed_lateness=120 s`: p-007 tiene delay=169 s → `too_late`.  
Con `allowed_lateness=180 s`: p-007 es aceptado como revisión.

---

## Implementación — Decisiones y trade-offs

### 1. Tiempo de evento (`parse_utc`)

Se usa `datetime.fromisoformat()` después de reemplazar el sufijo `Z` por `+00:00`. Esto garantiza objetos timezone-aware sin importar librerías externas.

**Trade-off:** `fromisoformat` en Python 3.12 acepta varios formatos ISO-8601 (con/sin microsegundos), lo que hace la función más robusta que una solución basada en `strptime` con formato fijo.

### 2. Ventanas fijas (`assign_fixed_window`)

Se calcula el inicio de ventana usando aritmética de epoch UNIX:

```
window_start_epoch = (posix_seconds // window_seconds) * window_seconds
```

Esto evita dependencias de zona horaria y funciona correctamente con cualquier timestamp UTC.

**Trade-off:** el cálculo asume que `window_seconds` divide uniformemente una hora. Para ventanas de tamaños arbitrarios que no alinean con la hora (ej. 90 s) la aritmética sigue siendo correcta porque usa segundos absolutos desde epoch.

### 3. Resumen determinista (`summarize_payments`)

Esta función pura de Python actúa como **oráculo** para el pipeline Beam. La lógica es:

1. Solo eventos `CONFIRMED` se acumulan en totales.
2. Deduplicación **por comercio**: el mismo `event_id` en comercios distintos no se interfieren.
3. `delay_seconds = arrival_time - event_time`.
4. Si `delay_seconds > allowed_lateness_seconds` → `too_late=True`, `accepted=False`.
5. Si el evento llegó después del `window_end` pero dentro de la tolerancia → `revision=True`.
6. La marca de visto se registra solo para eventos **aceptados** (no para rechazados o duplicados).

### 4. Pipeline Beam (`build_windowed_totals_pipeline`)

Secuencia:
```
Create → TimestampedValue → Filter(CONFIRMED) → FixedWindows → 
Map(merchant_id, amount) → CombinePerKey(sum) → Map(format with WindowParam)
```

`TimestampedValue` inyecta el `event_time` en segundos POSIX como timestamp Beam, lo que permite que `FixedWindows` lo clasifique correctamente independientemente del orden de llegada.

**Trade-off:** la deduplicación no está integrada en este pipeline simplificado (sin estado). Para producción, se agregaría `DeduplicatePayments` antes del `CombinePerKey`.

### 5. Deduplicación con estado (`DeduplicatePayments`)

Usa `SetStateSpec` para rastrear `event_id` por `merchant_id`. Beam garantiza que el estado es **local a cada clave**, por lo que dos comercios con el mismo `event_id` no se afectan.

El timer de `WATERMARK` se fija en `window.end`. Cuando el watermark supera ese punto:

- `expire()` llama `seen_ids.clear()`.
- El estado liberado no volverá a crecer para esa ventana cerrada.

**¿Por qué sin expiración el estado crece indefinidamente?**  
En runners persistentes (Flink, Dataflow), el estado vive mientras exista la clave. Sin limpieza activa, `seen_ids` acumula todos los IDs históricos de cada comercio, consumiendo storage sin límite y degradando el rendimiento al buscar duplicados.

### 6. Triggers (`build_trigger_policy`)

```python
AfterWatermark(
    early=AfterProcessingTime(10),  # estimación temprana a los 10 s
    late=AfterCount(1),             # revisión por cada evento tardío
)
```

Con `AccumulationMode.ACCUMULATING`: cada pane contiene el total acumulado completo, no solo el incremento.

**Trade-off:** el modo `ACCUMULATING` simplifica el sink (siempre puede hacer UPSERT con el valor más reciente). El modo `DISCARDING` sería más eficiente en storage pero requiere que el sink agregue incrementos.

### 7. Idempotencia del sink

La clave `merchant_id|window_start` identifica unívocamente el resultado lógico. En el modo `UPSERT`:

```python
sink[key] = row  # idempotente: mismo key → mismo resultado final
```

En el modo `POST` (append-only), cada reintento agrega una fila adicional, lo que requiere deduplicación posterior en las consultas.

**Trade-off:** el UPSERT requiere que el sink soporte semántica de reemplazo por clave (DynamoDB, Redis, BigQuery MERGE, PostgreSQL ON CONFLICT). Es más complejo de configurar pero garantiza exactamente-una-vez sin coordinación en el pipeline.

---

## Suite de pruebas

```
tests/test_assignment.py::test_parse_utc_returns_timezone_aware_datetime        PASSED
tests/test_assignment.py::test_assign_fixed_window_uses_event_time              PASSED
tests/test_assignment.py::test_duplicate_does_not_change_total                  PASSED
tests/test_assignment.py::test_deduplication_is_isolated_by_merchant            PASSED
tests/test_assignment.py::test_stateful_dofn_keeps_keys_isolated                PASSED
tests/test_assignment.py::test_out_of_order_event_uses_its_event_time_window    PASSED
tests/test_assignment.py::test_late_event_within_tolerance_is_a_revision        PASSED
tests/test_assignment.py::test_event_beyond_lateness_is_audited                 PASSED
tests/test_assignment.py::test_windowed_pipeline_produces_totals                PASSED
tests/test_assignment.py::test_trigger_policy_has_lateness_and_accumulating_panes PASSED
tests/test_assignment.py::test_retries_converge_to_one_materialized_entity      PASSED
tests/test_assignment.py::test_append_only_sink_materializes_every_attempt      PASSED
tests/test_assignment.py::test_timer_handler_clears_state                       PASSED

13 passed in ...s
```

---

## Estructura del repositorio

```
.
├── data/
│   └── payments.jsonl          # Dataset de pagos de prueba
├── tests/
│   ├── conftest.py             # Fixture que carga el notebook por AST
│   └── test_assignment.py      # Suite de pruebas obligatorias
├── notebook.py                 # Notebook Marimo con la implementación
├── pyproject.toml              # Dependencias (uv/pip)
├── Dockerfile                  # Imagen de producción
├── docker-compose.yml          # Servicios: notebook + tests
└── README.md                   # Este archivo
```
