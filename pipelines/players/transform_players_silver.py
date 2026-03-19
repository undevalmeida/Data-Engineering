"""
Transformação Bronze → Silver para jogadores (artilheiros).

Lê JSONs Bronze, normaliza e grava em data/silver/players/{ds}/.
"""
import json

from common.config import get_bronze_path, get_silver_path
from common.constants import COMPETITIONS


def _normalize_scorers(bronze: dict, competition_id: str) -> list[dict]:
    """Extrai e normaliza os artilheiros da resposta Bronze."""
    out = []
    for s in bronze.get("scorers", []):
        out.append({
            "competition_id": competition_id,
            "player_id": s.get("player", {}).get("id"),
            "player_name": s.get("player", {}).get("name"),
            "position": s.get("player", {}).get("position"),
            "team_id": s.get("team", {}).get("id"),
            "team_name": s.get("team", {}).get("name"),
            "goals": s.get("goals"),
            "assists": s.get("assists"),
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
            domain="players",
            competition_id=competition_id,
            filename=f"scorers_{competition_id}.json",
        )
        if not path.exists():
            continue
        with open(path, encoding="utf-8") as f:
            bronze = json.load(f)
        all_records.extend(_normalize_scorers(bronze, competition_id))

    out_path = get_silver_path(
        execution_date=execution_date,
        domain="players",
        filename="players.json",
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(all_records, f, ensure_ascii=False, indent=2)
