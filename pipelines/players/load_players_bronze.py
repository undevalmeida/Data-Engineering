"""
Carga da camada Bronze para jogadores (artilheiros).

Grava JSON bruto em data/bronze/players/{competition_id}/{ds}/.
"""
import json

from common.config import get_bronze_path
from common.constants import CONN_ID_FOOTBALL_DATA_ORG
from common.logging import get_logger

from pipelines.players.extract_players_api import extract_players

logger = get_logger(__name__)


def run_bronze(execution_date: str, conn_id: str = CONN_ID_FOOTBALL_DATA_ORG) -> None:
    """
    Extrai artilheiros via extract e grava na Bronze.

    Args:
        execution_date: Data da execução (YYYY-MM-DD).
        conn_id: ID da Connection do Airflow.
    """
    data = extract_players(conn_id=conn_id)
    for competition_id, payload in data.items():
        path = get_bronze_path(
            execution_date=execution_date,
            domain="players",
            competition_id=competition_id,
            filename=f"scorers_{competition_id}.json",
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    logger.info("Bronze players: %s competições", len(data))
