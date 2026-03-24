"""
Extração de partidas da API football-data.org.

Uma unidade de extração = um (competition_id, season). O paralelismo fica no nível
da DAG (uma task por unidade); o pipeline só expõe extract_one e lista de items.
"""
from datetime import datetime
from typing import Any

import requests

from common.airflow_variables import (
    get_competitions,
    get_football_data_org_conn_id,
    get_min_season_year,
)
from common.logging import get_logger

logger = get_logger(__name__)


def _max_season_year() -> int:
    """Último ano de temporada a extrair: ano civil atual − 1 (a temporada do ano em curso costuma estar incompleta)."""
    return datetime.utcnow().year - 1


def get_seasons(start_year: int | None = None) -> list[int]:
    """Seasons a extrair: de start_year até o ano anterior ao civil atual, inclusive (start_year via Variable ou default)."""
    if start_year is None:
        start_year = get_min_season_year()
    hi = _max_season_year()
    if start_year > hi:
        return []
    return list(range(start_year, hi + 1))


def get_match_items(conn_id: str | None = None) -> list[dict[str, Any]]:
    """
    Lista de itens (competition_id, season) para a DAG mapear em tasks.

    Não chama a API para montar a lista. Gera localmente o produto cartesiano
    competição x temporada para reduzir hits e evitar 429 antes da Bronze.

    Lista de competições e ano mínimo: Airflow Variables (ver `common/airflow_variables.py`) com
    fallback em `common.constants`.

    Retorna [{"competition_id": "PL", "season": 2017}, ...] para uso em task.expand().
    """
    _ = conn_id  # assinatura mantida por compatibilidade
    seasons = get_seasons()
    if not seasons:
        logger.warning(
            "get_match_items: nenhuma temporada no intervalo [min=%s .. max=%s] — "
            "nenhuma task bronze será agendada (ajuste football_data_org_min_season_year ou o relógio do worker).",
            get_min_season_year(),
            _max_season_year(),
        )
    items: list[dict[str, Any]] = []
    competition_ids = get_competitions()
    logger.info(
        "get_match_items: %s competição(ões) na lista: %s",
        len(competition_ids),
        competition_ids,
    )

    for cid in competition_ids:
        logger.info("Competição %s: temporadas alvo=%s", cid, seasons)
        for season in seasons:
            items.append({"competition_id": cid, "season": season})

    return items


def extract_matches_one(
    competition_id: str,
    season: int,
    conn_id: str | None = None,
) -> Any | None:
    """
    Extrai partidas para um (competition_id, season). Retorna payload ou None.

    Quando retorna 0 partidas, devolve None.
    Quando a chamada de API falha, levanta exceção para a task retryar/falhar (evita falso positivo).
    """
    from plugins.hooks import FootballDataOrgHook

    resolved_conn = conn_id or get_football_data_org_conn_id()
    try:
        hook = FootballDataOrgHook(football_data_org_conn_id=resolved_conn)
        logger.info(
            "Matches %s/%s: GET /v4/competitions/%s/matches?season=%s (paginado por limit/offset).",
            competition_id,
            season,
            competition_id,
            season,
        )
        if hasattr(hook, "get_competition_matches_all_pages"):
            payload = hook.get_competition_matches_all_pages(competition_id, season=season)
        else:
            # Compatibilidade com workers que ainda carregaram versão antiga do plugin.
            logger.warning(
                "Hook sem get_competition_matches_all_pages; usando get_competition_matches (uma página). "
                "Reinicie scheduler/worker para carregar o plugin atualizado."
            )
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
    except requests.HTTPError as e:
        status_code = e.response.status_code if e.response is not None else None
        if status_code in (403, 404):
            logger.warning(
                "Matches %s/%s: %s da API (fora do plano/permissão ou temporada inexistente). "
                "Pulando sem falhar a task e sem gravar arquivo.",
                competition_id,
                season,
                status_code,
            )
            return None
        logger.error(
            "Matches %s/%s: falha HTTP %s – %s. Task deve falhar para retry.",
            competition_id,
            season,
            status_code,
            e,
        )
        raise
    except Exception as e:
        logger.error(
            "Matches %s/%s: falha na API – %s. Task deve falhar para retry; sem mascarar como sucesso.",
            competition_id,
            season,
            e,
        )
        raise
