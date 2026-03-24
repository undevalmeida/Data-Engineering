"""
Carga da camada Bronze para partidas de competições.

Uma unidade = um arquivo: data/bronze/competition_matches/{PL_Premier_League}/{ds}/competition_matches_PL_2017.json
"""
import json
import os

from common.airflow_variables import get_football_data_org_conn_id
from common.config import get_bronze_path, get_data_base_path
from common.constants import DOMAIN_COMPETITION_MATCHES
from common.logging import get_logger

from pipelines.matches.extract_matches_api import extract_matches_one

logger = get_logger(__name__)


def run_bronze_one(
    execution_date: str,
    competition_id: str,
    season: int,
    conn_id: str | None = None,
) -> None:
    """
    Extrai partidas para um (competition_id, season) e grava um JSON.

    Nome do arquivo: competition_matches_{competition_id}_{season}.json
    Pasta: competition_matches/{PL_Premier_League}/ (nome legível).
    """
    get_data_base_path().mkdir(parents=True, exist_ok=True)
    payload = extract_matches_one(
        competition_id,
        season,
        conn_id=conn_id or get_football_data_org_conn_id(),
    )
    if not payload:
        logger.info(
            "Competition matches %s/%s: sem payload (API 403/erro ou 0 partidas) — arquivo não gravado. "
            "Base dados=%s DATA_BASE_PATH=%s",
            competition_id,
            season,
            get_data_base_path().resolve(),
            os.environ.get("DATA_BASE_PATH") or "(não definido; veja get_data_base_path no worker)",
        )
        return
    path = get_bronze_path(
        execution_date=execution_date,
        domain=DOMAIN_COMPETITION_MATCHES,
        competition_id=competition_id,
        filename=f"competition_matches_{competition_id}_{season}.json",
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    out = path.resolve()
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    logger.info(
        "Competition matches %s/%s: %s partidas gravadas em %s (execution_date=%s, base=%s, DATA_BASE_PATH=%s).",
        competition_id,
        season,
        len(payload.get("matches", [])),
        out,
        execution_date,
        get_data_base_path().resolve(),
        os.environ.get("DATA_BASE_PATH") or "(não definido)",
    )
