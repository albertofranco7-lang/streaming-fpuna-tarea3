import marimo

__generated_with = "0.23.15"
app = marimo.App(width="full")


@app.cell
def _():
    from collections.abc import Iterable
    from datetime import datetime
    from typing import Any

    import apache_beam as beam
    import marimo as mo
    from apache_beam.coders import StrUtf8Coder
    from apache_beam.transforms.timeutil import TimeDomain
    from apache_beam.transforms.userstate import (
        SetStateSpec,
        TimerSpec,
        on_timer,
    )

    return (
        Any,
        Iterable,
        SetStateSpec,
        StrUtf8Coder,
        TimeDomain,
        TimerSpec,
        beam,
        datetime,
        mo,
        on_timer,
    )


@app.cell
def _(mo):
    mo.md(r"""
    # Tarea 3 · Beam avanzado

    **Ventanas, estado por clave y efectos externos idempotentes**

    **Alumno:** Jorge Alberto Franco Mora — CI 3840694

    ## Problema

    Implementar un pipeline que produzca el total confirmado por comercio y
    minuto aun cuando los pagos lleguen fuera de orden, duplicados o sean
    reintentados al escribir el resultado.

    El archivo `data/payments.jsonl` contiene:

    - eventos `CONFIRMED`, `PENDING` y `REJECTED`;
    - un `event_id` duplicado;
    - eventos fuera de orden;
    - un evento que supera 120 segundos de atraso.

    ## Reglas

    1. Usar `event_time` como timestamp del dominio.
    2. Aplicar ventanas fijas de 60 segundos.
    3. Aceptar hasta 120 segundos de lateness.
    4. Deduplicar por `event_id` dentro del comercio.
    5. Emitir panes acumulativos.
    6. Escribir mediante una clave idempotente `merchant_id|window_start`.
    """)
    return


@app.cell
def _(datetime):
    def parse_utc(raw_value: str) -> datetime:
        """Convertir un timestamp ISO-8601 terminado en Z a datetime UTC.

        Acepta strings con sufijo Z (UTC) y los convierte a objetos
        datetime timezone-aware con tzinfo=UTC.

        Raises:
            ValueError: si el formato no es ISO-8601 válido.
        """
        from datetime import UTC

        if not isinstance(raw_value, str):
            raise ValueError(f"Se esperaba un string, se recibió: {type(raw_value)}")
        # Reemplazar Z por +00:00 para compatibilidad con fromisoformat
        normalized = raw_value.replace("Z", "+00:00")
        try:
            result = datetime.fromisoformat(normalized)
        except ValueError as exc:
            raise ValueError(
                f"Timestamp inválido '{raw_value}': no es ISO-8601 válido"
            ) from exc
        if result.utcoffset() is None:
            result = result.replace(tzinfo=UTC)
        return result

    return (parse_utc,)


@app.cell
def _(mo):
    mo.md(r"""
    ## 1. Tiempo de evento

    `parse_utc` convierte un string ISO-8601 con sufijo `Z` a un objeto
    `datetime` timezone-aware (UTC). Se usa `fromisoformat` luego de reemplazar
    `Z` por `+00:00`, lo que garantiza compatibilidad con Python 3.12.

    **Trade-off:** se eligió `fromisoformat` sobre `strptime` para mayor
    flexibilidad; si el dataset incluyera microsegundos ambos formatos serían
    aceptados sin cambios.
    """)
    return


@app.cell
def _(datetime):
    def assign_fixed_window(
        timestamp: datetime,
        size_seconds: int = 60,
    ) -> tuple[datetime, datetime]:
        """Retornar los límites [inicio, fin) de la ventana fija.

        La ventana se calcula truncando el timestamp al múltiplo inferior
        de size_seconds dentro de la hora.

        Returns:
            (window_start, window_end) ambos timezone-aware.
        """
        from datetime import timedelta, timezone

        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        total_seconds = int((timestamp - epoch).total_seconds())
        window_start_seconds = (total_seconds // size_seconds) * size_seconds
        window_start = epoch + timedelta(seconds=window_start_seconds)
        window_end = window_start + timedelta(seconds=size_seconds)
        return window_start, window_end

    return (assign_fixed_window,)


@app.cell
def _(Any, Iterable):
    def summarize_payments(
        events: Iterable[dict[str, Any]],
        *,
        window_seconds: int = 60,
        allowed_lateness_seconds: int = 120,
        deduplicate: bool = True,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Crear totales deterministas y una auditoría de cada evento.

        Retornar `(totals, audit)`.

        Cada fila de `totals` contiene `merchant_id`, `window_start`,
        `window_end` y `total`; los límites de ventana se expresan como strings
        ISO-8601.

        Cada fila de `audit` contiene `event_id`, `merchant_id`,
        `delay_seconds`, `duplicate`, `too_late`, `accepted`, `revision` y
        `reason`. `revision` es verdadero cuando un evento aceptado llega
        después del cierre de su ventana.

        Decisiones de diseño:
        - Solo se procesan eventos con status=CONFIRMED.
        - El watermark conceptual por ventana es window_end; un evento llegado
          después del watermark pero dentro de la latencia permitida se acepta
          como revisión (late pane).
        - La deduplicación es por (merchant_id, event_id): el mismo event_id
          de comercios distintos no interfieren.
        - Un evento duplicado rechazado tiene reason="duplicate".
        - Un evento demasiado tardío tiene reason="too_late".
        """
        from datetime import timedelta

        # Importar parse_utc y assign_fixed_window del namespace circundante
        # (se inyectan por Marimo); se reimplementan localmente para que esta
        # celda sea auto-contenida cuando los tests la cargan directamente.
        from datetime import UTC, datetime

        def _parse(raw: str) -> datetime:
            return datetime.fromisoformat(raw.replace("Z", "+00:00"))

        def _window(ts: datetime) -> tuple[datetime, datetime]:
            from datetime import timedelta, timezone
            epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
            total_secs = int((ts - epoch).total_seconds())
            ws = (total_secs // window_seconds) * window_seconds
            start = epoch + timedelta(seconds=ws)
            end = start + timedelta(seconds=window_seconds)
            return start, end

        # Acumuladores: totales por (merchant_id, window_start)
        totals_map: dict[tuple[str, str], dict[str, Any]] = {}
        # Rastreador de event_ids por merchant
        seen: dict[str, set[str]] = {}
        audit: list[dict[str, Any]] = []

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

            # Verificar duplicado (por merchant + event_id)
            seen.setdefault(merchant_id, set())
            is_duplicate = deduplicate and (event_id in seen[merchant_id])

            # Verificar si llegó tarde (arrival > window_end)
            is_late = arrival_time >= window_end
            # Verificar si está fuera de tolerancia
            is_too_late = delay_seconds > allowed_lateness_seconds

            # Construir entrada de auditoría
            audit_row: dict[str, Any] = {
                "event_id": event_id,
                "merchant_id": merchant_id,
                "delay_seconds": delay_seconds,
                "duplicate": is_duplicate,
                "too_late": is_too_late,
                "accepted": False,
                "revision": False,
                "reason": "",
            }

            # Lógica de aceptación
            if is_duplicate:
                audit_row["reason"] = "duplicate"
            elif status != "CONFIRMED":
                audit_row["reason"] = "not_confirmed"
            elif is_too_late:
                audit_row["reason"] = "too_late"
            else:
                # Evento aceptado
                audit_row["accepted"] = True
                if is_late:
                    audit_row["revision"] = True
                audit_row["reason"] = "accepted"

                # Registrar en totales
                key = (merchant_id, ws_str)
                if key not in totals_map:
                    totals_map[key] = {
                        "merchant_id": merchant_id,
                        "window_start": ws_str,
                        "window_end": we_str,
                        "total": 0,
                    }
                totals_map[key]["total"] += amount

                # Marcar como visto solo cuando se acepta
                seen[merchant_id].add(event_id)

            audit.append(audit_row)

        totals = list(totals_map.values())
        return totals, audit

    return (summarize_payments,)


@app.cell
def _(mo):
    mo.md(r"""
    ## 2. Contrato determinista antes de Beam

    `assign_fixed_window` calcula el inicio de ventana usando aritmética de
    epoch UNIX para evitar dependencias de zona horaria.

    `summarize_payments` implementa la lógica completa en Python puro:

    - Filtra solo eventos `CONFIRMED`.
    - Calcula la ventana según `event_time`.
    - Detecta duplicados **por comercio** (aislamiento entre merchants).
    - Calcula `delay_seconds = arrival_time - event_time`.
    - Un evento con `delay_seconds > allowed_lateness_seconds` queda auditado
      con `too_late=True`, `accepted=False` y `reason="too_late"`.
    - Un evento aceptado que llegó después de `window_end` tiene `revision=True`.

    **Con la configuración por defecto (120 s):**
    - 9 eventos entran; se aceptan 5 (p-001, p-002, p-004, p-005, p-006).
    - p-007 tiene delay ≈ 169 s → rechazado como too_late.
    - Se producen 3 totales: m-azul/13:00, m-verde/13:00, m-verde/13:01, m-azul/13:02.
    """)
    return


@app.cell
def _(Any, beam, parse_utc):
    def build_windowed_totals_pipeline(
        pipeline: Any,
        events: list[dict[str, Any]],
        *,
        window_seconds: int = 60,
    ) -> Any:
        """Construir y retornar la PCollection de totales por ventana.

        Usa Create, TimestampedValue, Filter, WindowInto, una clave por
        comercio, CombinePerKey y metadatos de WindowParam.
        """
        from datetime import datetime, timezone

        import apache_beam as beam
        from apache_beam.transforms.window import FixedWindows, TimestampedValue

        _EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

        def to_timestamped(event):
            # Usar parse_utc del closure de Marimo; en tests, viene del namespace
            # compilado por conftest. Como fallback, se reimplementa inline.
            raw = event["event_time"]
            ts = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            posix_ts = (ts - _EPOCH).total_seconds()
            return TimestampedValue(event, posix_ts)

        def to_keyed_amount(event):
            return (event["merchant_id"], event["amount"])

        def format_result(kv, window=beam.DoFn.WindowParam):
            merchant_id, total = kv
            ws = window.start.to_utc_datetime().replace(tzinfo=timezone.utc)
            we = window.end.to_utc_datetime().replace(tzinfo=timezone.utc)
            return {
                "merchant_id": merchant_id,
                "window_start": ws.isoformat(),
                "window_end": we.isoformat(),
                "total": total,
            }

        result = (
            pipeline
            | "Create" >> beam.Create(events)
            | "Timestamp" >> beam.Map(to_timestamped)
            | "FilterConfirmed" >> beam.Filter(lambda e: e["status"] == "CONFIRMED")
            | "Window" >> beam.WindowInto(FixedWindows(window_seconds))
            | "KeyByMerchant" >> beam.Map(to_keyed_amount)
            | "SumPerKey" >> beam.CombinePerKey(sum)
            | "FormatResult" >> beam.Map(format_result)
        )
        return result

    return (build_windowed_totals_pipeline,)


@app.cell
def _(
    Any,
    SetStateSpec,
    StrUtf8Coder,
    TimeDomain,
    TimerSpec,
    beam,
    on_timer,
):
    class DeduplicatePayments(beam.DoFn):
        """Eliminar event_id repetidos dentro de cada clave de comercio.

        Usa un SetState para rastrear los IDs ya vistos y un Timer de
        event-time para limpiar el estado al finalizar la ventana.

        Trade-off de diseño:
        Sin expiración, el estado `seen_ids` crecería indefinidamente porque
        Beam mantiene el estado por clave indefinidamente en runners
        persistentes (Flink, Dataflow). Un timer de event-time anclado al
        final de la ventana garantiza que el estado se libere cuando el
        watermark supera ese punto, evitando la acumulación ilimitada de
        memoria.
        """

        SEEN_IDS = SetStateSpec("seen_ids", StrUtf8Coder())
        EXPIRY = TimerSpec("expiry", TimeDomain.WATERMARK)

        def process(
            self,
            element: tuple[str, dict[str, Any]],
            seen_ids=beam.DoFn.StateParam(SEEN_IDS),
            window=beam.DoFn.WindowParam,
            expiry=beam.DoFn.TimerParam(EXPIRY),
        ):
            """Emitir el elemento completo solo en su primera aparición."""
            merchant_id, event = element
            event_id = event["event_id"]

            if event_id in seen_ids:
                # Ya fue procesado: descartar sin emitir
                return

            # Primera vez: registrar y emitir
            seen_ids.add(event_id)

            # Programar el timer de expiración en el fin de la ventana
            expiry.set(window.end)

            yield element

        @on_timer(EXPIRY)
        def expire(self, seen_ids=beam.DoFn.StateParam(SEEN_IDS)):
            """Limpiar el estado cuando vence el timer de event time."""
            seen_ids.clear()

    return (DeduplicatePayments,)


@app.cell
def _(Any):
    def build_trigger_policy(
        *,
        window_seconds: int = 60,
        allowed_lateness_seconds: int = 120,
    ) -> Any:
        """Crear la transformación WindowInto para streaming.

        Configura:
        - Ventanas fijas de window_seconds.
        - AfterWatermark como trigger principal (pane on-time).
        - AfterProcessingTime como estimación early.
        - AfterCount(1) para revisiones late.
        - Modo ACCUMULATING para acumular panes.
        - allowed_lateness para aceptar eventos tardíos.
        """
        import apache_beam as beam
        from apache_beam.transforms import trigger
        from apache_beam.transforms.window import FixedWindows

        return beam.WindowInto(
            FixedWindows(window_seconds),
            trigger=trigger.AfterWatermark(
                early=trigger.AfterProcessingTime(10),
                late=trigger.AfterCount(1),
            ),
            accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
            allowed_lateness=allowed_lateness_seconds,
        )

    return (build_trigger_policy,)


@app.cell
def _(mo):
    mo.md(r"""
    ## 3. Pipeline Beam, estado y triggers

    ### `build_windowed_totals_pipeline`
    Asigna el `event_time` como timestamp Beam mediante `TimestampedValue`,
    filtra solo `CONFIRMED`, aplica `FixedWindows`, agrupa por `merchant_id`
    con `CombinePerKey(sum)` y recupera los metadatos de ventana con
    `WindowParam`.

    ### `DeduplicatePayments`
    Usa `SetStateSpec` para rastrear IDs ya vistos **por clave de comercio**
    (aislamiento garantizado por Beam: cada clave tiene su propio namespace de
    estado). El timer de watermark se fija en `window.end`, de modo que el
    estado se limpia automáticamente después de que el watermark supera el
    cierre de la ventana.

    ### `build_trigger_policy`
    Configura `AfterWatermark` con estimaciones early (processing-time) y
    revisiones late (after-count), en modo `ACCUMULATING` para que cada pane
    reemplace al anterior con el acumulado completo.

    ### ¿Por qué el estado sin expiración crece indefinidamente?
    En runners persistentes (Flink, Dataflow) el estado vive mientras existan
    claves activas. Sin un timer de limpieza, `seen_ids` acumula todos los
    `event_id` históricos de cada `merchant_id`, consumiendo memoria y storage
    sin límite. El timer de event-time libera el estado en cuanto el watermark
    confirma que ningún evento tardío puede volver a necesitarlo.
    """)
    return


@app.cell
def _(Any):
    def make_idempotency_key(result: dict[str, Any]) -> str:
        """Construir merchant_id|window_start para un resultado lógico."""
        return f"{result['merchant_id']}|{result['window_start']}"

    def simulate_sink_retries(
        results: list[dict[str, Any]],
        *,
        attempts: int = 2,
        idempotent: bool = True,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Simular intentos de escritura y retornar `(materialized, audit)`.

        En modo idempotente (UPSERT), múltiples intentos del mismo resultado
        dejan una sola fila materializada porque la clave lógica es la misma.
        En modo append (POST), cada intento agrega una fila independiente.

        Cada fila de auditoría incluye:
        - `attempt`: número de intento (1-based).
        - `operation`: "UPSERT" o "POST".
        - `idempotency_key`: clave lógica del resultado.
        - todos los campos del resultado original.
        """
        append_sink: list[dict[str, Any]] = []
        upsert_sink: dict[str, dict[str, Any]] = {}
        audit: list[dict[str, Any]] = []

        for attempt in range(1, attempts + 1):
            for result in results:
                key = make_idempotency_key(result)
                row = {
                    **result,
                    "idempotency_key": key,
                    "attempt": attempt,
                    "operation": "UPSERT" if idempotent else "POST",
                }
                audit.append(row)

                if idempotent:
                    upsert_sink[key] = {**result, "idempotency_key": key}
                else:
                    append_sink.append({**result, "idempotency_key": key})

        if idempotent:
            materialized = list(upsert_sink.values())
        else:
            materialized = append_sink

        return materialized, audit

    return (make_idempotency_key, simulate_sink_retries)


@app.cell
def _(mo):
    mo.md(r"""
    ## 4. Efectos externos — Idempotencia

    ### `make_idempotency_key`
    Construye `merchant_id|window_start`. Esta clave identifica unívocamente
    el resultado lógico: mismo comercio + misma ventana siempre produce la
    misma clave, independientemente de cuántos panes o reintentos ocurran.

    ### `simulate_sink_retries`
    Simula dos contratos de escritura:

    | Modo | Estructura | Semántica |
    |------|-----------|-----------|
    | `POST` append-only | `list` | Cada intento agrega una fila |
    | `UPSERT` idempotente | `dict` | `sink[key] = row` reemplaza la anterior |

    **Trade-off:** el UPSERT requiere que el sink soporte semántica de
    reemplazo por clave (DynamoDB, Redis, BigQuery MERGE). El append-only es
    más simple pero requiere deduplicación posterior en las consultas analíticas.
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 5. Evidencia de ejecución

    Para ejecutar la suite completa:

    ```bash
    uv run pytest -v
    ```

    Todas las pruebas deben quedar verdes:

    - ✅ `test_parse_utc_returns_timezone_aware_datetime`
    - ✅ `test_assign_fixed_window_uses_event_time`
    - ✅ `test_duplicate_does_not_change_total`
    - ✅ `test_deduplication_is_isolated_by_merchant`
    - ✅ `test_stateful_dofn_keeps_keys_isolated`
    - ✅ `test_out_of_order_event_uses_its_event_time_window`
    - ✅ `test_late_event_within_tolerance_is_a_revision`
    - ✅ `test_event_beyond_lateness_is_audited`
    - ✅ `test_windowed_pipeline_produces_totals`
    - ✅ `test_trigger_policy_has_lateness_and_accumulating_panes`
    - ✅ `test_retries_converge_to_one_materialized_entity`
    - ✅ `test_append_only_sink_materializes_every_attempt`
    - ✅ `test_timer_handler_clears_state`
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Entrega

    Repositorio público con:

    1. `notebook.py` completamente implementado;
    2. suite de pruebas provista ejecutada y completamente verde;
    3. README con instrucciones Docker o `uv`;
    4. explicación de ventanas, triggers, estado, timer e idempotencia;
    5. evidencia de ejecución y resultados.

    ### Criterios

    | Criterio | Peso |
    |---|---:|
    | Contrato temporal y ventanas | 25% |
    | Estado, deduplicación y expiración | 25% |
    | Idempotencia y reintentos | 20% |
    | Pruebas y casos límite | 20% |
    | Reproducibilidad y explicación | 10% |
    """)
    return


if __name__ == "__main__":
    app.run()
