"""
Agregação Silver → Gold para times.

Lê Silver, gera resumo por competição (contagem, lista) em data/gold/teams/{ds}/.
"""
import json
from collections import defaultdict

from common.config import get_silver_path, get_gold_path


def run_gold(execution_date: str) -> None:
    """
    Lê Silver de times e grava agregados na Gold.

    Args:
        execution_date: Data da execução (YYYY-MM-DD).
    """
    silver_path = get_silver_path(
        execution_date=execution_date,
        domain="teams",
        filename="teams.json",
    )
    if not silver_path.exists():
        out_path = get_gold_path(execution_date=execution_date, domain="teams", filename="summary.json")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump([], f, indent=2)
        return

    with open(silver_path, encoding="utf-8") as f:
        teams = json.load(f)

    by_comp = defaultdict(list)
    for row in teams:
        by_comp[row["competition_id"]].append(row)
    summary = [
        {"competition_id": comp_id, "total_teams": len(rows), "teams": [r.get("name") for r in rows[:20]]}
        for comp_id, rows in sorted(by_comp.items())
    ]

    out_path = get_gold_path(execution_date=execution_date, domain="teams", filename="summary.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
