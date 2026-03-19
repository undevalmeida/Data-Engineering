"""
Extração e transformação de dados da API football-data.org.

Fluxo Medallion: Bronze (raw) → Silver (curated) → Gold (agregado).
Todas as funções leem/escrevem em arquivos; nenhum dado é retornado para XCom.
"""
import json
from pathlib import Path

from utils.paths import (
    get_bronze_path,
    get_gold_path,
    get_silver_path,
)

# Competições extraídas (códigos da API football-data.org)
COMPETITIONS = [
    "WC",   # FIFA World Cup
    "CL",   # UEFA Champions League
    "BL1",  # Bundesliga
    "DED",  # Eredivisie
    "BSA",  # Campeonato Brasileiro Série A
    "PD",   # Primera Division
    "FL1",  # Ligue 1
    "ELC",  # Championship
    "PPL",  # Primeira Liga
    "EC",   # European Championship
    "SA",   # Serie A
    "PL",   # Premier League
]

# Recursos extraídos por competição (nome do recurso -> método do hook)
RESOURCE_HOOK_METHODS = {
    "standings": "get_standings",
    "matches": "get_competition_matches",
    "teams": "get_competition_teams",
    "scorers": "get_scorers",
}


def run_bronze_extraction(
    execution_date: str,
    conn_id: str | None = None,
) -> None:
    """
    Extrai dados da API football-data.org e grava na camada Bronze.

    Para cada competição e recurso (standings, matches, teams, scorers),
    chama o FootballDataOrgHook, grava a resposta bruta em JSON em
    data/bronze/{resource}/{competition_id}/{execution_date}/{resource}_{competition_id}.json.
    Erros de API (ex.: 403) são logados e a extração segue para o próximo item.

    Args:
        execution_date: Data da execução no formato YYYY-MM-DD.
        conn_id: ID da Connection do Airflow (default: football_data_org).
    """
    from common.constants import CONN_ID_FOOTBALL_DATA_ORG
    from plugins.hooks import FootballDataOrgHook

    conn_id = conn_id or CONN_ID_FOOTBALL_DATA_ORG
    hook = FootballDataOrgHook(football_data_org_conn_id=conn_id)

    for competition_id in COMPETITIONS:
        for resource, method_name in RESOURCE_HOOK_METHODS.items():
            method = getattr(hook, method_name)
            path = get_bronze_path(
                execution_date=execution_date,
                resource=resource,
                competition_id=competition_id,
                filename=f"{resource}_{competition_id}.json",
            )
            path.parent.mkdir(parents=True, exist_ok=True)
            try:
                if resource == "scorers":
                    payload = method(competition_id)
                else:
                    payload = method(competition_id)
                with open(path, "w", encoding="utf-8") as f:
                    json.dump(payload, f, ensure_ascii=False, indent=2)
            except Exception as e:
                import logging
                logging.getLogger(__name__).warning(
                    "Bronze: falha %s %s: %s", resource, competition_id, e
                )


def _normalize_standings(bronze: dict, competition_id: str) -> list[dict]:
    """Extrai e normaliza as tabelas de classificação da resposta Bronze."""
    out = []
    for standing in bronze.get("standings", []):
        for row in standing.get("table", []):
            out.append({
                "competition_id": competition_id,
                "position": row.get("position"),
                "team_id": row.get("team", {}).get("id"),
                "team_name": row.get("team", {}).get("name"),
                "playedGames": row.get("playedGames"),
                "won": row.get("won"),
                "draw": row.get("draw"),
                "lost": row.get("lost"),
                "points": row.get("points"),
                "goalsFor": row.get("goalsFor"),
                "goalsAgainst": row.get("goalsAgainst"),
            })
    return out


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


NORMALIZERS = {
    "standings": _normalize_standings,
    "teams": _normalize_teams,
    "scorers": _normalize_scorers,
    "matches": _normalize_matches,
}


def run_silver_transformation(execution_date: str) -> None:
    """
    Lê os arquivos Bronze do execution_date, normaliza e grava na camada Silver.

    Para cada recurso, agrega todos os JSONs das competições em uma única
    lista de registros normalizados e grava em
    data/silver/{resource}/{execution_date}/{resource}.json.
    """
    for resource in RESOURCE_HOOK_METHODS:
        normalizer = NORMALIZERS[resource]
        all_records = []
        for competition_id in COMPETITIONS:
            path = get_bronze_path(
                execution_date=execution_date,
                resource=resource,
                competition_id=competition_id,
                filename=f"{resource}_{competition_id}.json",
            )
            if not path.exists():
                continue
            with open(path, encoding="utf-8") as f:
                bronze = json.load(f)
            all_records.extend(normalizer(bronze, competition_id))

        out_path = get_silver_path(
            execution_date=execution_date,
            resource=resource,
            filename=f"{resource}.json",
        )
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(all_records, f, ensure_ascii=False, indent=2)


def run_gold_aggregation(execution_date: str) -> None:
    """
    Lê os arquivos Silver do execution_date, agrega e grava na camada Gold.

    Gera um resumo por competição (standings resumido e contagem de partidas)
    em data/gold/{execution_date}/summary.json.
    """
    summary = []
    standings_path = get_silver_path(
        execution_date=execution_date,
        resource="standings",
        filename="standings.json",
    )
    matches_path = get_silver_path(
        execution_date=execution_date,
        resource="matches",
        filename="matches.json",
    )

    if standings_path.exists():
        with open(standings_path, encoding="utf-8") as f:
            standings = json.load(f)
        from collections import defaultdict
        by_comp = defaultdict(list)
        for row in standings:
            by_comp[row["competition_id"]].append(row)
        for comp_id, rows in sorted(by_comp.items()):
            top3 = sorted(rows, key=lambda x: (x.get("position") or 99))[:3]
            summary.append({
                "competition_id": comp_id,
                "top3": [
                    {"position": r["position"], "team": r.get("team_name"), "points": r.get("points")}
                    for r in top3
                ],
            })

    if matches_path.exists():
        with open(matches_path, encoding="utf-8") as f:
            matches = json.load(f)
        from collections import Counter
        match_count = Counter(m["competition_id"] for m in matches)
        for entry in summary:
            entry["total_matches"] = match_count.get(entry["competition_id"], 0)

    out_path = get_gold_path(execution_date=execution_date, filename="summary.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
