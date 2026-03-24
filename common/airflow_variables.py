"""
Configuração operacional via Airflow Variables, com fallback para `common.constants`.

Boas práticas:
- **Connection** (`football_data_org`): host, token/senha — nunca commitar segredos.
- **Variables**: listas de competições, ano mínimo, *nome* da Connection — ajustáveis por ambiente sem redeploy.

Fora do Airflow (tests, scripts locais), os getters retornam sempre os defaults de `constants`.
"""
from __future__ import annotations

from typing import Any

import common.constants as _constants
from common.logging import get_logger

# Defaults alinhados a `constants`; getattr evita ImportError se o container tiver `constants` antigo.
COMPETITIONS = _constants.COMPETITIONS
CONN_ID_FOOTBALL_DATA_ORG = getattr(_constants, "CONN_ID_FOOTBALL_DATA_ORG", "football_data_org")
DEFAULT_MIN_SEASON_YEAR = getattr(_constants, "DEFAULT_MIN_SEASON_YEAR", 2017)

logger = get_logger(__name__)

# Chaves sugeridas no Admin → Variables (evitar typos: usar estas constantes na documentação).
VAR_FOOTBALL_DATA_ORG_CONNECTION_ID = "football_data_org_connection_id"
VAR_FOOTBALL_DATA_ORG_COMPETITIONS = "football_data_org_competitions"
VAR_FOOTBALL_DATA_ORG_MIN_SEASON_YEAR = "football_data_org_min_season_year"
VAR_FOOTBALL_DATA_ORG_QUALITY_ALLOW_EMPTY = "football_data_org_quality_allow_empty"
VAR_FOOTBALL_DATA_ORG_BRONZE_PARTITION_TZ = "football_data_org_bronze_partition_tz"


def _safe_get_variable(key: str, default: Any = None) -> Any:
    """Lê Variable do Airflow; em falha (sem Airflow, chave inexistente, etc.) retorna default."""
    try:
        from airflow.models import Variable

        return Variable.get(key, default_var=default)
    except Exception as e:  # ImportError, AirflowException, etc.
        logger.debug("Variable %s indisponível (%s); usando default.", key, e)
        return default


def get_football_data_org_conn_id() -> str:
    """
    ID da Connection HTTP da API (não o segredo — o token fica na Connection).

    Variable opcional: `football_data_org_connection_id` (string).
    """
    raw = _safe_get_variable(VAR_FOOTBALL_DATA_ORG_CONNECTION_ID, default=None)
    if raw is not None and str(raw).strip():
        return str(raw).strip()
    return CONN_ID_FOOTBALL_DATA_ORG


def get_competitions() -> list[str]:
    """
    Lista de códigos de competição (ex.: PL, CL).

    Variable opcional: `football_data_org_competitions` (JSON array de strings).
    """
    try:
        from airflow.models import Variable

        v = Variable.get(
            VAR_FOOTBALL_DATA_ORG_COMPETITIONS,
            default_var="[]",
            deserialize_json=True,
        )
        if isinstance(v, list) and len(v) > 0:
            out = [str(x).strip() for x in v if str(x).strip()]
            if out:
                return out
    except Exception as e:
        logger.debug("Variável %s indisponível ou inválida (%s); usando COMPETITIONS.", VAR_FOOTBALL_DATA_ORG_COMPETITIONS, e)
    return list(COMPETITIONS)


def get_min_season_year() -> int:
    """
    Ano mínimo do parâmetro `season` (fallback de intervalo e clamp).

    Variable opcional: `football_data_org_min_season_year` (inteiro, ex.: 2017).
    """
    raw = _safe_get_variable(VAR_FOOTBALL_DATA_ORG_MIN_SEASON_YEAR, default=None)
    if raw is not None and str(raw).strip():
        try:
            return int(str(raw).strip())
        except ValueError:
            logger.warning("Variable %s inválida (%r); usando %s.", VAR_FOOTBALL_DATA_ORG_MIN_SEASON_YEAR, raw, DEFAULT_MIN_SEASON_YEAR)
    return DEFAULT_MIN_SEASON_YEAR


def get_quality_allow_empty() -> bool:
    """
    Se True, `run_quality_checks` não falha quando não há JSON na data do run (só aviso no log).

    Útil quando a API pode devolver 403/429 ou 0 partidas para todos os pares naquele run.

    Variable opcional: `football_data_org_quality_allow_empty` (true/false, 1/0).
    Default: False — sem Variable, 0 arquivos na partição **falha** a task de qualidade (DAG vermelha).
    Defina `true` apenas para ambientes em que vazio é aceitável.
    """
    raw = _safe_get_variable(VAR_FOOTBALL_DATA_ORG_QUALITY_ALLOW_EMPTY, default="false")
    if raw is None:
        return False
    return str(raw).strip().lower() in ("1", "true", "yes", "y", "on")


def get_bronze_partition_timezone() -> str:
    """
    Fuso IANA usado para converter `logical_date` (UTC) em YYYY-MM-DD das pastas Bronze.

    Ex.: com `America/Sao_Paulo`, um run às 23h de 23/03 no Brasil pode ainda ser 24/03 UTC;
    a pasta segue o **dia civil no Brasil** (23/03), não o dia UTC.

    Variable opcional: `football_data_org_bronze_partition_tz` (ex.: `America/Sao_Paulo`, `UTC`).
    Default: `America/Sao_Paulo`.
    """
    raw = _safe_get_variable(VAR_FOOTBALL_DATA_ORG_BRONZE_PARTITION_TZ, default="America/Sao_Paulo")
    name = (str(raw).strip() if raw is not None else "") or "America/Sao_Paulo"
    try:
        from zoneinfo import ZoneInfo

        ZoneInfo(name)
        return name
    except Exception:
        logger.warning("Timezone inválido %r; usando America/Sao_Paulo.", name)
        return "America/Sao_Paulo"
