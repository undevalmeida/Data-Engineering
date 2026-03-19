"""
Transformação Bronze → Silver para times.

Lê JSONs Bronze, normaliza e grava em data/silver/teams/{ds}/.
"""
import json

from common.config import get_bronze_path, get_silver_path
from common.constants import COMPETITIONS


def _normalize_teams(bronze: dict, competition_id: str) -> list[dict]:
    """Extrai e normaliza os times da resposta Bronze."""
    out = []
    for t in bronze.get("teams", []):
        out.append({
            "competition_id": competition_id,
            "id": t.get("id"),
            "name": t.get("name"),
            "shortName": t.get("shortName"),
            "tla": t.get("tla"),
            "crest": t.get("crest"),
        })
    return out


def run_silver(execution_date: str) -> None:
    """
    Lê Bronze do execution_date, normaliza e grava na Silver.

    Args:
        execution_date: Data da execução (YYYY-MM-DD).
    """
    all_records = []
    for competition_id in COMPETITIONS:
        path = get_bronze_path(
            execution_date=execution_date,
            domain="teams",
            competition_id=competition_id,
            filename=f"teams_{competition_id}.json",
        )
        if not path.exists():
            continue
        with open(path, encoding="utf-8") as f:
            bronze = json.load(f)
        all_records.extend(_normalize_teams(bronze, competition_id))

    out_path = get_silver_path(
        execution_date=execution_date,
        domain="teams",
        filename="teams.json",
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(all_records, f, ensure_ascii=False, indent=2)
