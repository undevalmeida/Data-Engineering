"""
Tasks reutilizáveis do pipeline football-data.org (Medallion).

Cada task é decorada com @task (TaskFlow API) e obtém execution_date
do contexto da execução. Dados trafegam apenas por arquivos em data/ (sem XCom).
"""
from airflow.decorators import task
from airflow.operators.python import get_current_context

from common.constants import CONN_ID_FOOTBALL_DATA_ORG


@task
def bronze_extract(conn_id: str = CONN_ID_FOOTBALL_DATA_ORG) -> str:
    """
    Extrai dados da API football-data.org e grava na camada Bronze.

    Para cada competição e recurso (standings, matches, teams, scorers),
    chama o hook, persiste JSON em data/bronze/{resource}/{competition_id}/{ds}/.
    Usa execution_date (ds) do contexto da run; não recebe nem retorna dados via XCom.

    Args:
        conn_id: ID da Connection do Airflow para a API.

    Returns:
        Status leve ("ok") para o Airflow; dados ficam em arquivos.
    """
    from utils.football_extract import run_bronze_extraction

    context = get_current_context()
    ds = context["ds"]
    run_bronze_extraction(execution_date=ds, conn_id=conn_id)
    return "ok"


@task
def silver_transform() -> str:
    """
    Normaliza os dados Bronze e grava na camada Silver.

    Lê os arquivos Bronze do execution_date (ds) do contexto,
    aplica normalização por recurso e grava em data/silver/{resource}/{ds}/.

    Returns:
        Status leve ("ok"); dados trafegam apenas por arquivos.
    """
    from utils.football_extract import run_silver_transformation

    context = get_current_context()
    ds = context["ds"]
    run_silver_transformation(execution_date=ds)
    return "ok"


@task
def gold_aggregate() -> str:
    """
    Agrega os dados Silver e grava na camada Gold.

    Lê Silver do execution_date (ds) do contexto, gera summary
    (top 3 por competição, total de partidas) em data/gold/{ds}/summary.json.

    Returns:
        Status leve ("ok"); dados trafegam apenas por arquivos.
    """
    from utils.football_extract import run_gold_aggregation

    context = get_current_context()
    ds = context["ds"]
    run_gold_aggregation(execution_date=ds)
    return "ok"
