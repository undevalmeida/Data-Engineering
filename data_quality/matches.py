"""
Qualidade de dados para competition matches (camada Bronze).

Valida: arquivos existem, JSON válido, estrutura esperada (chave "matches", lista).
Não cria pastas Bronze — quem grava é o pipeline de carga.
"""
import json
import os

from common.config import get_data_base_path
from common.constants import DOMAIN_COMPETITION_MATCHES
from common.logging import get_logger

logger = get_logger(__name__)


class DataQualityError(Exception):
    """Erro de qualidade dos dados (estrutura ou conteúdo)."""
    pass


def run_quality_checks(
    execution_date: str,
    domain: str = DOMAIN_COMPETITION_MATCHES,
    *,
    allow_empty: bool | None = None,
) -> None:
    """
    Valida os arquivos Bronze de competition matches para a data de execução.

    - Percorre data/bronze/competition_matches/{PL_Premier_League}/{execution_date}/*.json
    - Cada arquivo deve ser JSON válido com chave "matches" (lista).

    Args:
        execution_date: Data da execução (YYYY-MM-DD).
        domain: Domínio (default competition_matches).
        allow_empty: Se True, não levanta erro quando não há arquivos nessa data (só log).
            Se None, usa a Airflow Variable `football_data_org_quality_allow_empty` (default estrito).
    """
    if allow_empty is None:
        from common.airflow_variables import get_quality_allow_empty

        allow_empty = get_quality_allow_empty()
    base = get_data_base_path()
    bronze_matches = base / "bronze" / domain
    try:
        bronze_ready = bronze_matches.is_dir()
    except OSError as e:
        raise DataQualityError(
            f"Não foi possível acessar o diretório Bronze {bronze_matches}: {e}"
        ) from e

    checked = 0
    errors: list[str] = []

    if not bronze_ready:
        logger.warning(
            "Qualidade: domínio Bronze ainda inexistente (%s). Nenhum arquivo gravado pela extração "
            "ou DATA_BASE_PATH distinto entre workers — confira logs de bronze_one e volume partilhado.",
            bronze_matches.resolve(),
        )

    for competition_dir in sorted(bronze_matches.iterdir()) if bronze_ready else []:
        if not competition_dir.is_dir():
            continue
        exec_dir = competition_dir / execution_date
        if not exec_dir.exists():
            continue
        try:
            json_paths = sorted(exec_dir.glob("*.json"))
        except OSError as e:
            errors.append(f"{exec_dir}: falha ao listar arquivos ({type(e).__name__}) – {e}")
            continue
        for path in json_paths:
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
            except UnicodeDecodeError as e:
                errors.append(f"{path}: não é UTF-8 válido – {e}")
            except OSError as e:
                errors.append(f"{path}: falha de leitura/I-O ({type(e).__name__}) – {e}")

    if errors:
        raise DataQualityError(
            f"Qualidade de dados falhou ({len(errors)} problema(s)):\n" + "\n".join(errors[:20])
            + ("\n..." if len(errors) > 20 else "")
        )
    if checked == 0:
        # Diagnóstico: datas de pasta existentes (ajuda a achar divergência de `ds` vs arquivos antigos)
        date_folders: set[str] = set()
        if bronze_ready:
            for competition_dir in bronze_matches.iterdir():
                if not competition_dir.is_dir():
                    continue
                for child in competition_dir.iterdir():
                    if child.is_dir():
                        date_folders.add(child.name)
        sample_dates = sorted(date_folders)[:15]
        hint = ""
        if sample_dates:
            hint = (
                f" Pastas de data encontradas em alguma competição (amostra): {sample_dates}. "
                "Se a data procurada não aparece, o run atual não gravou nada (API 403/429, 0 partidas) "
                "ou a task Bronze usou outra data — confira logs de `bronze_one` e use a mesma data em `ds`."
            )
        else:
            hint = (
                " Nenhuma subpasta de data sob competições — nenhuma escrita Bronze neste volume "
                "(API sem dados/403) ou workers usam DATA_BASE_PATH diferente do scheduler."
            )
        env_base = os.environ.get("DATA_BASE_PATH") or "(não definido)"
        msg = (
            f"Nenhum arquivo JSON de competition_matches encontrado em {bronze_matches.resolve()} "
            f"para a data {execution_date} (partição fuso da Variable). Base resolvida={base.resolve()} "
            f"DATA_BASE_PATH={env_base}.{hint} "
            "Confira logs de bronze_one (caminho absoluto gravado) e montagem de volume entre tasks."
        )
        if allow_empty:
            logger.warning(
                "Qualidade: 0 arquivos para %s — allow_empty=True (Variable ou parâmetro); DAG segue. %s",
                execution_date,
                msg,
            )
            return
        raise DataQualityError(msg)
    logger.info("Qualidade competition_matches: %s arquivo(s) validado(s) para %s.", checked, execution_date)
