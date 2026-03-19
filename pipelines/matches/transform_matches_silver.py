"""
Transformação Bronze → Silver para partidas.

Lê JSONs Bronze, normaliza (campos achatados, tipos) e grava em data/silver/matches/{ds}/.
"""
import json

from common.config import get_bronze_path, get_silver_path
from common.constants import COMPETITIONS


def _normalize_matches(bronze: dict, competition_id: str) -> list[dict]:
    """Extrai e normaliza as partidas da resposta Bronze."""
    out = []
    for m in bronze.get("matches", []):
        out.append({
            "competition_id": competition_id,
            "id": m.get("id"),
            "utcDate": m.get("utcDate"),
            "status": m.get("status"),
            "homeTeam_id": m.get("homeTeam", {}).get("id"),
            "homeTeam_name": m.get("homeTeam", {}).get("name"),
            "awayTeam_id": m.get("awayTeam", {}).get("id"),
            "awayTeam_name": m.get("awayTeam", {}).get("name"),
            "score_home": m.get("score", {}).get("fullTime", {}).get("home"),
            "score_away": m.get("score", {}).get("fullTime", {}).get("away"),
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
            domain="matches",
            competition_id=competition_id,
            filename=f"matches_{competition_id}.json",
        )
        if not path.exists():
            continue
        with open(path, encoding="utf-8") as f:
            bronze = json.load(f)
        all_records.extend(_normalize_matches(bronze, competition_id))

    out_path = get_silver_path(
        execution_date=execution_date,
        domain="matches",
        filename="matches.json",
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(all_records, f, ensure_ascii=False, indent=2)
