"""
Agregação Silver → Gold para jogadores.

Lê Silver, gera top artilheiros por competição em data/gold/players/{ds}/.
"""
import json
from collections import defaultdict

from common.config import get_silver_path, get_gold_path


def run_gold(execution_date: str) -> None:
    """
    Lê Silver de jogadores e grava agregados na Gold (top artilheiros por competição).

    Args:
        execution_date: Data da execução (YYYY-MM-DD).
    """
    silver_path = get_silver_path(
        execution_date=execution_date,
        domain="players",
        filename="players.json",
    )
    if not silver_path.exists():
        out_path = get_gold_path(execution_date=execution_date, domain="players", filename="top_scorers.json")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump([], f, indent=2)
        return

    with open(silver_path, encoding="utf-8") as f:
        players = json.load(f)

    by_comp = defaultdict(list)
    for row in players:
        by_comp[row["competition_id"]].append(row)
    top_per_comp = []
    for comp_id, rows in sorted(by_comp.items()):
        top5 = sorted(rows, key=lambda x: -(x.get("goals") or 0))[:5]
        top_per_comp.append({
            "competition_id": comp_id,
            "top_scorers": [{"name": r.get("player_name"), "goals": r.get("goals"), "team": r.get("team_name")} for r in top5],
        })

    out_path = get_gold_path(execution_date=execution_date, domain="players", filename="top_scorers.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(top_per_comp, f, ensure_ascii=False, indent=2)
