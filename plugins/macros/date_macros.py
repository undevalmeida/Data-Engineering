"""
Macros de data para uso em templates Airflow (Jinja).

Registrar em airflow.cfg [core] custom_ui_components ou usar em DAGs
como funções disponíveis em templates (via get_template_context ou variáveis).
"""
from datetime import datetime, timedelta


def ds_add(ds: str, days: int) -> str:
    """Soma dias a uma data no formato YYYY-MM-DD."""
    d = datetime.strptime(ds, "%Y-%m-%d")
    return (d + timedelta(days=days)).strftime("%Y-%m-%d")


def ds_sub(ds: str, days: int) -> str:
    """Subtrai dias de uma data no formato YYYY-MM-DD."""
    return ds_add(ds, -days)


def first_day_of_month(ds: str) -> str:
    """Retorna o primeiro dia do mês da data dada (YYYY-MM-DD)."""
    d = datetime.strptime(ds, "%Y-%m-%d")
    return d.replace(day=1).strftime("%Y-%m-%d")
