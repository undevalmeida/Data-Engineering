"""
Carga da camada Bronze para times.

Grava JSON bruto em data/bronze/teams/{competition_id}/{ds}/.
"""
import json

from common.config import get_bronze_path
from common.constants import CONN_ID_FOOTBALL_DATA_ORG
from common.logging import get_logger

from pipelines.teams.extract_teams_api import extract_teams

logger = get_logger(__name__)


def run_bronze(execution_date: str, conn_id: str = CONN_ID_FOOTBALL_DATA_ORG) -> None:
    """
    Extrai times via extract e grava na Bronze.

    Args:
        execution_date: Data da execução (YYYY-MM-DD).
        conn_id: ID da Connection do Airflow.
    """
    data = extract_teams(conn_id=conn_id)
    for competition_id, payload in data.items():
        path = get_bronze_path(
            execution_date=execution_date,
            domain="teams",
            competition_id=competition_id,
            filename=f"teams_{competition_id}.json",
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    logger.info("Bronze teams: %s competições", len(data))
