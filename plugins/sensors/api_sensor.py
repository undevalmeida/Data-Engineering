"""
Sensor reutilizável: verifica se uma API está disponível (GET no endpoint retorna 200).

Para outra API no futuro: mude apenas endpoint e conn_id (e opcionalmente auth_header_name).
Auth vem da Connection (password ou extra); se auth_header_name for None, faz GET sem auth.
"""
import json
import requests
from airflow.hooks.base import BaseHook
from airflow.sensors.base import BaseSensorOperator


class ApiReadySensor(BaseSensorOperator):
    """
    Verifica se a API responde 200. Reutilizável: troque endpoint e conn_id para outra API.

    Exemplo football-data.org:
        ApiReadySensor(
            endpoint="https://api.football-data.org/v4/competitions/PL",
            conn_id="football_data_org",
        )
    Exemplo outra API (sem auth): auth_header_name=None.

    Args:
        endpoint: URL a testar (ex.: https://api.exemplo.com/v1/health).
        conn_id: ID da Connection no Airflow (host/token ficam na connection).
        auth_header_name: Nome do header de auth (ex.: "X-Auth-Token", "Authorization").
            Valor vem da Connection (password ou extra). Se None, não envia header de auth.
        timeout: Timeout do GET em segundos.
    """

    template_fields = ("endpoint",)

    def __init__(
        self,
        endpoint: str,
        conn_id: str,
        *,
        auth_header_name: str | None = "X-Auth-Token",
        timeout: int = 10,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.endpoint = endpoint
        self.conn_id = conn_id
        self.auth_header_name = auth_header_name
        self.timeout = timeout

    def _get_headers(self) -> dict[str, str]:
        """Monta headers a partir da Connection (password ou extra JSON)."""
        conn = BaseHook.get_connection(self.conn_id)
        token = conn.password or ""
        if conn.extra:
            try:
                extra = json.loads(conn.extra) if isinstance(conn.extra, str) else conn.extra
                token = token or extra.get("X-Auth-Token") or extra.get("x_auth_token") or extra.get("token") or ""
            except (json.JSONDecodeError, TypeError):
                pass
        if not self.auth_header_name or not token:
            return {}
        return {self.auth_header_name: token}

    def poke(self, context) -> bool:
        """GET no endpoint; True se status 200."""
        try:
            headers = self._get_headers()
            resp = requests.get(self.endpoint, headers=headers, timeout=self.timeout)
            return resp.status_code == 200
        except Exception:
            return False
