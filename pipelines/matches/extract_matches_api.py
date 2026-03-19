"""
Extração de partidas da API football-data.org.

Uma unidade de extração = um (competition_id, season). O paralelismo fica no nível
da DAG (uma task por unidade); o pipeline só expõe extract_one e lista de items.
"""
from datetime import datetime
from typing import Any

from common.constants import COMPETITIONS, CONN_ID_FOOTBALL_DATA_ORG
from common.logging import get_logger

MIN_SEASON_YEAR = 2017

logger = get_logger(__name__)


def get_seasons(start_year: int = MIN_SEASON_YEAR) -> list[int]:
    """Seasons a extrair: de start_year até o ano atual, inclusive."""
    current_year = datetime.utcnow().year
    return list(range(start_year, current_year + 1))


def get_match_items() -> list[dict[str, Any]]:
    """
    Lista de itens (competition_id, season) para a DAG mapear em tasks.

    Retorna [{"competition_id": "PL", "season": 2017}, ...] para uso em task.expand().
    """
    seasons = get_seasons()
    return [
        {"competition_id": cid, "season": season}
        for cid in COMPETITIONS
        for season in seasons
    ]


def extract_matches_one(
    competition_id: str,
    season: int,
    conn_id: str = CONN_ID_FOOTBALL_DATA_ORG,
) -> Any | None:
    """
    Extrai partidas para um (competition_id, season). Retorna payload ou None.

    Quando falha ou retorna 0 partidas, registra no log o motivo (ex.: 403 = liga fora do plano).
    """
    from plugins.hooks import FootballDataOrgHook

    try:
        hook = FootballDataOrgHook(football_data_org_conn_id=conn_id)
        payload = hook.get_competition_matches(competition_id, season=season)
        matches = payload.get("matches") or []
        if not matches:
            logger.info(
                "Matches %s/%s: API retornou 0 partidas (liga/season pode estar fora do plano ou sem dados).",
                competition_id,
                season,
            )
            return None
        return payload
    except Exception as e:
        logger.warning(
            "Matches %s/%s: falha na API – %s (ex.: 403 = liga não disponível no seu plano).",
            competition_id,
            season,
            e,
        )
        return None
