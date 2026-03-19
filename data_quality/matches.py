"""
Qualidade de dados para competition matches (camada Bronze).

Valida: arquivos existem, JSON válido, estrutura esperada (chave "matches", lista).
"""
import json
from pathlib import Path

from common.config import get_data_base_path
from common.constants import DOMAIN_COMPETITION_MATCHES
from common.logging import get_logger

logger = get_logger(__name__)


class DataQualityError(Exception):
    """Erro de qualidade dos dados (estrutura ou conteúdo)."""
    pass


def run_quality_checks(execution_date: str, domain: str = DOMAIN_COMPETITION_MATCHES) -> None:
    """
    Valida os arquivos Bronze de competition matches para a data de execução.

    - Percorre data/bronze/competition_matches/{PL_Premier_League}/{execution_date}/*.json
    - Cada arquivo deve ser JSON válido com chave "matches" (lista).

    Args:
        execution_date: Data da execução (YYYY-MM-DD).
        domain: Domínio (default competition_matches).
    """
    base = get_data_base_path()
    bronze_matches = base / "bronze" / domain
    if not bronze_matches.exists():
        raise DataQualityError(
            f"Pasta Bronze não encontrada: {bronze_matches}. "
            "Execute a extração antes da checagem de qualidade."
        )

    checked = 0
    errors: list[str] = []

    for competition_dir in sorted(bronze_matches.iterdir()):
        if not competition_dir.is_dir():
            continue
        exec_dir = competition_dir / execution_date
        if not exec_dir.exists():
            continue
        for path in sorted(exec_dir.glob("*.json")):
            try:
                with open(path, encoding="utf-8") as f:
                    data = json.load(f)
                if "matches" not in data:
                    errors.append(f"{path}: falta chave 'matches'")
                    continue
                if not isinstance(data["matches"], list):
                    errors.append(f"{path}: 'matches' deve ser lista, obteve {type(data['matches']).__name__}")
                    continue
                checked += 1
            except json.JSONDecodeError as e:
                errors.append(f"{path}: JSON inválido – {e}")

    if errors:
        raise DataQualityError(
            f"Qualidade de dados falhou ({len(errors)} problema(s)):\n" + "\n".join(errors[:20])
            + ("\n..." if len(errors) > 20 else "")
        )
    if checked == 0:
        raise DataQualityError(
            f"Nenhum arquivo JSON de competition_matches encontrado em {bronze_matches} "
            f"para a data {execution_date}. Verifique se a extração gerou dados."
        )
    logger.info("Qualidade competition_matches: %s arquivo(s) validado(s) para %s.", checked, execution_date)
