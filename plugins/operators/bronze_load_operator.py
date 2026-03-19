"""
Operator que carrega dados na camada Bronze.

Delega para a função run_bronze do pipeline do domínio (matches, players, teams).
"""
from airflow.models import BaseOperator

from common.constants import CONN_ID_FOOTBALL_DATA_ORG


class BronzeLoadOperator(BaseOperator):
    """
    Operador que executa a carga Bronze de um domínio.

    Template fields permitem passar execution_date e conn_id via Jinja.
    """

    template_fields = ("execution_ds", "conn_id", "domain")

    def __init__(
        self,
        domain: str,
        execution_ds: str | None = None,
        conn_id: str = CONN_ID_FOOTBALL_DATA_ORG,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.domain = domain
        self.execution_ds = execution_ds
        self.conn_id = conn_id

    def execute(self, context):
        """Executa run_bronze do pipeline correspondente ao domain."""
        ds = self.execution_ds or context["ds"]
        if self.domain == "matches":
            from pipelines.matches.load_matches_bronze import run_bronze
        elif self.domain == "players":
            from pipelines.players.load_players_bronze import run_bronze
        elif self.domain == "teams":
            from pipelines.teams.load_teams_bronze import run_bronze
        else:
            raise ValueError(f"Domain não suportado: {self.domain}")
        run_bronze(execution_date=ds, conn_id=self.conn_id)
