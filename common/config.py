"""
Configuração central da plataforma (paths do Data Lake e ambiente).

Centraliza o diretório base e a construção de caminhos Bronze/Silver/Gold
para que DAGs e pipelines usem a mesma convenção sem depender de XCom.
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
    if not default.exists():
        project_root = Path(__file__).resolve().parents[1]
        return project_root / "data"
    return default


def get_bronze_path(
    execution_date: str,
    domain: str,
    competition_id: str,
    filename: str | None = None,
) -> Path:
    """
    Path para a camada Bronze.

    Estrutura: {base}/bronze/{domain}/{competition_folder}/{execution_date}/[filename]
    Para domain competition_matches, competition_id é mapeado para pasta legível (ex.: PL -> PL_Premier_League).
    """
    from common.constants import COMPETITION_FOLDER_NAMES, DOMAIN_COMPETITION_MATCHES

    base = get_data_base_path()
    segment = (
        COMPETITION_FOLDER_NAMES.get(competition_id, competition_id)
        if domain == DOMAIN_COMPETITION_MATCHES
        else competition_id
    )
    folder = base / "bronze" / domain / segment / execution_date
    if filename:
        return folder / filename
    return folder


def get_silver_path(
    execution_date: str,
    domain: str,
    filename: str | None = None,
) -> Path:
    """
    Path para a camada Silver.

    Estrutura: {base}/silver/{domain}/{execution_date}/[filename]
    """
    base = get_data_base_path()
    folder = base / "silver" / domain / execution_date
    if filename:
        return folder / filename
    return folder


def get_gold_path(
    execution_date: str,
    domain: str,
    filename: str | None = None,
) -> Path:
    """
    Path para a camada Gold.

    Estrutura: {base}/gold/{domain}/{execution_date}/[filename]
    """
    base = get_data_base_path()
    folder = base / "gold" / domain / execution_date
    if filename:
        return folder / filename
    return folder
