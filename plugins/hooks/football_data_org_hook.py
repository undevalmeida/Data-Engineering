"""
Hook para a API football-data.org (api.football-data.org/v4).

Connection no Airflow:
- conn_id: football_data_org (constante CONN_ID_FOOTBALL_DATA_ORG)
- conn_type: http
- host: api.football-data.org
- password: token X-Auth-Token (https://www.football-data.org/client/register)

"""
import json
import time
from typing import Any

import requests
from airflow.hooks.base import BaseHook

from common.constants import CONN_ID_FOOTBALL_DATA_ORG


class FootballDataOrgHook(BaseHook):
    """
    Hook para a API football-data.org v4.

    Site: https://www.football-data.org
    Documentação: https://www.football-data.org/documentation/quickstart
    """

    conn_name_attr = "football_data_org_conn_id"
    default_conn_name = CONN_ID_FOOTBALL_DATA_ORG
    conn_type = "http"
    hook_name = "Football-Data.org API"

    def __init__(
        self,
        football_data_org_conn_id: str = default_conn_name,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.football_data_org_conn_id = football_data_org_conn_id
        self._session: requests.Session | None = None
        self._base_url: str = ""

    def get_conn(self) -> requests.Session:
        """Retorna uma sessão HTTP configurada com autenticação."""
        if self._session is not None:
            return self._session

        conn = BaseHook.get_connection(self.football_data_org_conn_id)

        token = None
        if conn.extra:
            try:
                extra = json.loads(conn.extra) if isinstance(conn.extra, str) else conn.extra
                token = extra.get("X-Auth-Token") or extra.get("x_auth_token")
            except (json.JSONDecodeError, TypeError):
                pass
        if not token and conn.password:
            token = conn.password

        if not token:
            raise ValueError(
                "Token da API football-data.org não encontrado. "
                "Configure a Connection 'password' ou extra['X-Auth-Token']."
            )

        host = conn.host or "api.football-data.org"
        schema = conn.schema or "https"
        self._base_url = f"{schema}://{host}/v4"

        self._session = requests.Session()
        self._session.headers.update({"X-Auth-Token": token})
        return self._session

    def _run(
        self,
        endpoint: str,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Executa requisição e retorna JSON, com retry para 429/503."""
        self.get_conn()
        url = f"{self._base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        req_headers = {**(headers or {})}
        max_attempts = 8
        response = None
        for attempt in range(max_attempts):
            response = self._session.request(
                method=method, url=url, params=params, headers=req_headers or None, **kwargs
            )
            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                try:
                    wait_seconds = int(retry_after) if retry_after else min(6 * (attempt + 1), 120)
                except ValueError:
                    wait_seconds = min(6 * (attempt + 1), 120)
                time.sleep(wait_seconds)
                continue
            if response.status_code == 503:
                time.sleep(min(2 ** attempt, 60))
                continue
            response.raise_for_status()
            return response.json()
        response.raise_for_status()
        return response.json()

    def get_areas(self, area_id: int | None = None) -> Any:
        """Lista áreas ou uma área específica."""
        path = f"areas/{area_id}" if area_id else "areas"
        return self._run(path)

    def get_competitions(self, areas: str | None = None, **params: Any) -> Any:
        """Lista competições. areas: ids separados por vírgula."""
        params = params or {}
        if areas:
            params["areas"] = areas
        return self._run("competitions", params=params or None)

    def get_competition(self, competition_id: str) -> Any:
        """Retorna uma competição específica (ex: PL, CL, SA)."""
        return self._run(f"competitions/{competition_id}")

    def get_standings(self, competition_id: str, **params: Any) -> Any:
        """Tabela de classificação de uma competição."""
        return self._run(
            f"competitions/{competition_id}/standings",
            params=params or None,
        )

    def get_competition_matches(self, competition_id: str, **params: Any) -> Any:
        """Lista partidas de uma competição."""
        return self._run(
            f"competitions/{competition_id}/matches",
            params=params or None,
        )

    def get_competition_matches_all_pages(self, competition_id: str, **params: Any) -> dict[str, Any]:
        """
        Lista partidas de uma competição agregando páginas via limit/offset.
        """
        merged: list[Any] = []
        offset = 0
        limit = 100
        first: dict[str, Any] | None = None
        total_expected: int | None = None
        for _ in range(500):
            page_params = dict(params) if params else {}
            page_params["limit"] = limit
            page_params["offset"] = offset
            payload = self._run(
                f"competitions/{competition_id}/matches",
                params=page_params,
            )
            if first is None:
                first = payload
                count = (payload.get("resultSet") or {}).get("count")
                try:
                    total_expected = int(count) if count is not None else None
                except (TypeError, ValueError):
                    total_expected = None
            chunk = payload.get("matches") or []
            merged.extend(chunk)
            if total_expected is not None and len(merged) >= total_expected:
                break
            if not chunk or len(chunk) < limit:
                break
            offset += limit
        if first is None:
            return {"matches": [], "filters": params or {}, "resultSet": {}}
        out = dict(first)
        out["matches"] = merged
        return out

    def get_competition_teams(self, competition_id: str, season: str | None = None) -> Any:
        """Lista times de uma competição."""
        params = {"season": season} if season else None
        return self._run(
            f"competitions/{competition_id}/teams",
            params=params,
        )

    def get_scorers(
        self, competition_id: str, limit: int | None = None, season: str | None = None
    ) -> Any:
        """Lista artilheiros de uma competição."""
        params = {}
        if limit is not None:
            params["limit"] = limit
        if season:
            params["season"] = season
        return self._run(
            f"competitions/{competition_id}/scorers",
            params=params or None,
        )

    def get_team(self, team_id: int) -> Any:
        """Retorna um time específico."""
        return self._run(f"teams/{team_id}")

    def get_teams(self, limit: int | None = None, offset: int | None = None) -> Any:
        """Lista times."""
        params = {}
        if limit is not None:
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset
        return self._run("teams", params=params or None)

    def get_team_matches(self, team_id: int, **params: Any) -> Any:
        """Lista partidas de um time."""
        return self._run(f"teams/{team_id}/matches", params=params or None)

    def get_match(self, match_id: int) -> Any:
        """Retorna uma partida específica (GET /matches/{id})."""
        return self._run(f"matches/{match_id}")

    def get_match_head2head(
        self, match_id: int, limit: int | None = None, **params: Any
    ) -> Any:
        """Histórico confronto direto entre os times da partida (GET /matches/{id}/head2head)."""
        params = dict(params) if params else {}
        if limit is not None:
            params["limit"] = limit
        return self._run(f"matches/{match_id}/head2head", params=params or None)

    def get_matches(
        self,
        *,
        unfold_lineups: bool = False,
        unfold_goals: bool = False,
        unfold_bookings: bool = False,
        unfold_subs: bool = False,
        **params: Any,
    ) -> Any:
        """
        Lista partidas (GET /matches).

        Filtros: competitions, dateFrom, dateTo, status, etc.
        Headers opcionais: X-Unfold-Lineups, X-Unfold-Goals, X-Unfold-Bookings, X-Unfold-Subs.
        """
        headers = {}
        if unfold_lineups:
            headers["X-Unfold-Lineups"] = "true"
        if unfold_goals:
            headers["X-Unfold-Goals"] = "true"
        if unfold_bookings:
            headers["X-Unfold-Bookings"] = "true"
        if unfold_subs:
            headers["X-Unfold-Subs"] = "true"
        return self._run(
            "matches",
            params=params or None,
            headers=headers if headers else None,
        )

    def get_person(self, person_id: int) -> Any:
        """Retorna uma pessoa/jogador específico (GET /persons/{id})."""
        return self._run(f"persons/{person_id}")

    def get_person_matches(
        self, person_id: int, limit: int | None = None, **params: Any
    ) -> Any:
        """Lista partidas de uma pessoa/jogador (GET /persons/{id}/matches)."""
        params = dict(params) if params else {}
        if limit is not None:
            params["limit"] = limit
        return self._run(f"persons/{person_id}/matches", params=params or None)
