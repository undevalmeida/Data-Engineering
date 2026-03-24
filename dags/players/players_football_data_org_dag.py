"""
Pipeline Medallion para jogadores/artilheiros (players).

Fonte: football-data.org. Fluxo: init → bronze → silver → gold → finish.
"""
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context

from common.airflow_context import context_ds_yyyy_mm_dd

DAG_ID = "players_football_data_org_dag"


@dag(
    dag_id=DAG_ID,
    description="[football-data.org] Jogadores (artilheiros): Bronze → Silver → Gold",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["football_data_org", "players", "medallion"],
)
def players_football_data_org_dag():
    init = EmptyOperator(task_id="init")
    finish = EmptyOperator(task_id="finish")

    @task
    def bronze():
        context = get_current_context()
        from pipelines.players.load_players_bronze import run_bronze
        run_bronze(execution_date=context_ds_yyyy_mm_dd(context))
        return "ok"

    @task
    def silver():
        context = get_current_context()
        from pipelines.players.transform_players_silver import run_silver
        run_silver(execution_date=context_ds_yyyy_mm_dd(context))
        return "ok"

    @task
    def gold():
        context = get_current_context()
        from pipelines.players.aggregate_players_gold import run_gold
        run_gold(execution_date=context_ds_yyyy_mm_dd(context))
        return "ok"

    init >> bronze() >> silver() >> gold() >> finish


players_football_data_org_dag()
