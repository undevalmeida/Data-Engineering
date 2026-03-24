"""
Helpers de contexto Airflow para paths e datas consistentes entre tasks.

A data das pastas Bronze usa o fuso configurável — o Airflow trabalha em UTC em `logical_date`;
sem conversão, o “dia” da pasta pode ser o dia seguinte em relação ao calendário local (ex.: Brasil).
"""
from __future__ import annotations

from typing import Any


def context_ds_yyyy_mm_dd(context: dict[str, Any]) -> str:
    """
    Retorna YYYY-MM-DD para pastas Bronze/qualidade no **fuso da partição** (Variable
    `football_data_org_bronze_partition_tz`, default `America/Sao_Paulo`).

    Usa `logical_date` / `data_interval_start` (UTC) e converte para o fuso configurado.
    Inclui fallbacks para `dag_run` / `ti` (tasks mapeadas ou contextos reduzidos).
    Se só existir `ds`, interpreta como meia-noite UTC e converte (fallback).
    """
    from common.airflow_variables import get_bronze_partition_timezone

    tz_name = get_bronze_partition_timezone()

    logical = context.get("logical_date") or context.get("data_interval_start")
    if logical is None:
        dr = context.get("dag_run")
        if dr is not None:
            logical = getattr(dr, "logical_date", None) or getattr(
                dr, "data_interval_start", None
            )
    if logical is None:
        ti = context.get("ti")
        if ti is not None:
            dr = getattr(ti, "dag_run", None)
            if dr is not None:
                logical = getattr(dr, "logical_date", None) or getattr(
                    dr, "data_interval_start", None
                )
    if logical is not None:
        if hasattr(logical, "in_timezone"):
            return logical.in_timezone(tz_name).strftime("%Y-%m-%d")
        from zoneinfo import ZoneInfo

        dt = logical
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=ZoneInfo("UTC"))
        return dt.astimezone(ZoneInfo(tz_name)).strftime("%Y-%m-%d")

    ds = context.get("ds")
    if ds:
        try:
            import pendulum

            return pendulum.parse(str(ds).strip(), tz="UTC").in_timezone(tz_name).strftime("%Y-%m-%d")
        except Exception:
            return str(ds)

    raise ValueError(
        "Contexto Airflow sem ds, logical_date ou data_interval_start — não é possível montar o path Bronze."
    )
