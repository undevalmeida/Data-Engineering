"""
Pipeline de partidas por competição (competition matches) – camada Bronze.

Fluxo: init → check_api → get_match_items → bronze_one.expand → quality_check → finish.
"""
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context

from common.airflow_context import context_ds_yyyy_mm_dd
from common.airflow_variables import get_football_data_org_conn_id

DAG_ID = "competition_matches_football_data_org_dag"


@dag(
    dag_id=DAG_ID,
    description="[football-data.org] Partidas por competição (Bronze); uma task por competição+season",
    schedule="@yearly",
    catchup=False,
    start_date=datetime(2025, 1, 1),
    max_active_tasks=2,
    tags=["football_data_org", "competition_matches", "bronze"],
)
def competition_matches_football_data_org_dag():
    init = EmptyOperator(task_id="init")
    finish = EmptyOperator(task_id="finish")

    @task(task_id="check_api")
    def check_api() -> str:
        """Verifica se a fonte existe e a API está funcionando (token + GET)."""
        from plugins.hooks import FootballDataOrgHook
        hook = FootballDataOrgHook(football_data_org_conn_id=get_football_data_org_conn_id())
        hook.get_competition("PL")
        return "ok"

    @task(task_id="get_match_items")
    def get_match_items() -> list[dict]:
        """Lista (competition_id, season) para mapear em tasks."""
        from pipelines.matches.extract_matches_api import get_match_items as _get_items
        return _get_items()

    @task(task_id="bronze_one", retries=3, retry_delay=timedelta(seconds=90))
    def bronze_one(item: dict) -> None:
        """Extrai e grava Bronze para um (competition_id, season)."""
        context = get_current_context()
        from pipelines.matches.load_matches_bronze import run_bronze_one
        run_bronze_one(
            execution_date=context_ds_yyyy_mm_dd(context),
            competition_id=item["competition_id"],
            season=item["season"],
        )

    @task(task_id="quality_check")
    def quality_check() -> str:
        """Valida qualidade dos arquivos Bronze de competition_matches (JSON, estrutura)."""
        context = get_current_context()
        from data_quality.matches import run_quality_checks
        run_quality_checks(execution_date=context_ds_yyyy_mm_dd(context))
        return "ok"

    items = get_match_items()
    bronze_tasks = bronze_one.expand(item=items)
    init >> check_api() >> items >> bronze_tasks >> quality_check() >> finish


competition_matches_football_data_org_dag()
