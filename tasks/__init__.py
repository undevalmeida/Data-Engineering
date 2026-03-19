"""
Tasks reutilizáveis (TaskFlow API).

Agrupa tasks decoradas com @task para uso em uma ou mais DAGs,
seguindo o padrão de reuso e responsabilidade única.
"""
from tasks.football_tasks import (
    bronze_extract,
    silver_transform,
    gold_aggregate,
)

__all__ = [
    "bronze_extract",
    "silver_transform",
    "gold_aggregate",
]
