"""
Carga da camada Bronze para partidas de competições.

Uma unidade = um arquivo: data/bronze/competition_matches/{PL_Premier_League}/{ds}/competition_matches_PL_2017.json
"""
import json

from common.config import get_bronze_path
from common.constants import CONN_ID_FOOTBALL_DATA_ORG, DOMAIN_COMPETITION_MATCHES
from common.logging import get_logger

from pipelines.matches.extract_matches_api import extract_matches_one

logger = get_logger(__name__)


def run_bronze_one(
    execution_date: str,
    competition_id: str,
    season: int,
    conn_id: str = CONN_ID_FOOTBALL_DATA_ORG,
) -> None:
    """
    Extrai partidas para um (competition_id, season) e grava um JSON.

    Nome do arquivo: competition_matches_{competition_id}_{season}.json
    Pasta: competition_matches/{PL_Premier_League}/ (nome legível).
    """
    payload = extract_matches_one(competition_id, season, conn_id=conn_id)
    if not payload:
        logger.info("Competition matches %s/%s: sem dados, arquivo não gravado.", competition_id, season)
        return
    path = get_bronze_path(
        execution_date=execution_date,
        domain=DOMAIN_COMPETITION_MATCHES,
        competition_id=competition_id,
        filename=f"competition_matches_{competition_id}_{season}.json",
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    logger.info("Competition matches %s/%s: %s partidas gravadas.", competition_id, season, len(payload.get("matches", [])))
