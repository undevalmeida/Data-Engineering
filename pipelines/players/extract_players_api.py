"""
Extração de artilheiros (scorers) da API football-data.org.

Retorna dados brutos por competição para a camada Bronze.
"""
from typing import Any

from common.constants import COMPETITIONS, CONN_ID_FOOTBALL_DATA_ORG


def extract_players(conn_id: str = CONN_ID_FOOTBALL_DATA_ORG) -> dict[str, Any]:
    """
    Extrai artilheiros de todas as competições configuradas (API football-data.org).

    Args:
        conn_id: ID da Connection do Airflow (football_data_org).

    Returns:
        Dicionário {competition_id: payload_json}.
    """
    from plugins.hooks import FootballDataOrgHook

    hook = FootballDataOrgHook(football_data_org_conn_id=conn_id)
    out = {}
    for competition_id in COMPETITIONS:
        try:
            out[competition_id] = hook.get_scorers(competition_id)
        except Exception:
            continue
    return out
