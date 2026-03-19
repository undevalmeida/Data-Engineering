"""
Configuração de paths do Data Lake (arquitetura Medallion).

Centraliza o diretório base e a construção de caminhos para Bronze, Silver e Gold,
garantindo que as DAGs e tasks usem a mesma convenção sem depender de XCom.
"""
import os
from pathlib import Path


def get_data_base_path() -> Path:
    """
    Retorna o diretório raiz do Data Lake.

    Em Docker (PYTHONPATH=/opt/airflow), o default é /opt/airflow/data.
    Pode ser sobrescrito pela variável de ambiente DATA_BASE_PATH.
    """
    default = Path("/opt/airflow/data")
    env_path = os.environ.get("DATA_BASE_PATH")
    if env_path:
        return Path(env_path)
    # Fallback para desenvolvimento local: pasta data ao lado do projeto
    if not default.exists():
        project_root = Path(__file__).resolve().parents[1]
        return project_root / "data"
    return default


def get_bronze_path(
    execution_date: str,
    resource: str,
    competition_id: str,
    filename: str | None = None,
) -> Path:
    """
    Retorna o path para gravar ou ler dados na camada Bronze.

    Estrutura: {base}/bronze/{resource}/{competition_id}/{execution_date}/[filename]

    Args:
        execution_date: Data da execução (YYYY-MM-DD).
        resource: Recurso (standings, matches, teams, scorers).
        competition_id: Código da competição (ex.: PL, CL).
        filename: Nome do arquivo (ex.: standings_PL.json). Se None, retorna o diretório.

    Returns:
        Path do arquivo ou do diretório.
    """
    base = get_data_base_path()
    folder = base / "bronze" / resource / competition_id / execution_date
    if filename:
        return folder / filename
    return folder


def get_silver_path(
    execution_date: str,
    resource: str,
    filename: str | None = None,
) -> Path:
    """
    Retorna o path para gravar ou ler dados na camada Silver.

    Estrutura: {base}/silver/{resource}/{execution_date}/[filename]

    Args:
        execution_date: Data da execução (YYYY-MM-DD).
        resource: Recurso (standings, matches, teams, scorers).
        filename: Nome do arquivo. Se None, retorna o diretório.

    Returns:
        Path do arquivo ou do diretório.
    """
    base = get_data_base_path()
    folder = base / "silver" / resource / execution_date
    if filename:
        return folder / filename
    return folder


def get_gold_path(
    execution_date: str,
    filename: str | None = None,
) -> Path:
    """
    Retorna o path para gravar ou ler dados na camada Gold.

    Estrutura: {base}/gold/{execution_date}/[filename]

    Args:
        execution_date: Data da execução (YYYY-MM-DD).
        filename: Nome do arquivo. Se None, retorna o diretório.

    Returns:
        Path do arquivo ou do diretório.
    """
    base = get_data_base_path()
    folder = base / "gold" / execution_date
    if filename:
        return folder / filename
    return folder
