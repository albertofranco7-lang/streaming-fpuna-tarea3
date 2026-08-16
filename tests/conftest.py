"""Carga las funciones definidas dentro de las celdas del notebook Marimo."""

from __future__ import annotations

import ast
from collections.abc import Iterable
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import apache_beam as beam
import pytest
from apache_beam.coders import StrUtf8Coder
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.transforms.userstate import SetStateSpec, TimerSpec, on_timer

NOTEBOOK_PATH = Path(__file__).parents[1] / "notebook.py"

ASSIGNMENT_DEFINITIONS = {
    "parse_utc",
    "assign_fixed_window",
    "summarize_payments",
    "build_windowed_totals_pipeline",
    "DeduplicatePayments",
    "build_trigger_policy",
    "make_idempotency_key",
    "simulate_sink_retries",
}


def _definition_nodes() -> list[ast.FunctionDef | ast.ClassDef]:
    tree = ast.parse(NOTEBOOK_PATH.read_text(encoding="utf-8"))
    definitions: list[ast.FunctionDef | ast.ClassDef] = []

    for cell in tree.body:
        if not isinstance(cell, ast.FunctionDef) or cell.name != "_":
            continue
        for statement in cell.body:
            if (
                isinstance(statement, (ast.FunctionDef, ast.ClassDef))
                and statement.name in ASSIGNMENT_DEFINITIONS
            ):
                definitions.append(statement)

    found = {definition.name for definition in definitions}
    missing = ASSIGNMENT_DEFINITIONS - found
    if missing:
        raise AssertionError(
            f"Faltan definiciones obligatorias en notebook.py: {sorted(missing)}"
        )
    return definitions


@pytest.fixture(scope="session")
def solution() -> SimpleNamespace:
    """Compila solo las definiciones de la tarea, no la aplicación Marimo."""
    namespace: dict[str, Any] = {
        "__name__": "assignment_notebook",
        "Any": Any,
        "Iterable": Iterable,
        "SetStateSpec": SetStateSpec,
        "StrUtf8Coder": StrUtf8Coder,
        "TimeDomain": TimeDomain,
        "TimerSpec": TimerSpec,
        "beam": beam,
        "datetime": datetime,
        "on_timer": on_timer,
    }
    module = ast.Module(body=_definition_nodes(), type_ignores=[])
    ast.fix_missing_locations(module)
    exec(compile(module, NOTEBOOK_PATH, "exec"), namespace)

    return SimpleNamespace(
        **{
            name: namespace[name]
            for name in sorted(ASSIGNMENT_DEFINITIONS)
        }
    )
