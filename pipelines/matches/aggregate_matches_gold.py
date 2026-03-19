"""
Agregação Silver → Gold para partidas.

Lê Silver, gera resumos (ex.: total de partidas por competição, por status) em data/gold/matches/{ds}/.
"""
import json
from collections import Counter

from common.config import get_silver_path, get_gold_path


def run_gold(execution_date: str) -> None:
    """
    Lê Silver de partidas e grava agregados na Gold.

    Args:
        execution_date: Data da execução (YYYY-MM-DD).
    """
    silver_path = get_silver_path(
        execution_date=execution_date,
        domain="matches",
        filename="matches.json",
    )
    if not silver_path.exists():
        out_path = get_gold_path(execution_date=execution_date, domain="matches", filename="summary.json")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump([], f, indent=2)
        return

    with open(silver_path, encoding="utf-8") as f:
        matches = json.load(f)

    by_competition = Counter(m["competition_id"] for m in matches)
    by_status = Counter(m.get("status") for m in matches)
    summary = {
        "execution_date": execution_date,
        "total_matches": len(matches),
        "by_competition": dict(by_competition),
        "by_status": dict(by_status),
    }

    out_path = get_gold_path(execution_date=execution_date, domain="matches", filename="summary.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
