"""
Constantes da plataforma (competições, recursos, defaults).

Evita magic strings e centraliza listas usadas por pipelines e DAGs.
"""

# Ano mínimo de temporada (parâmetro `season` na API) quando não há override via Airflow Variable.
DEFAULT_MIN_SEASON_YEAR = 2017

# Competições football-data.org (códigos API).
# No plano free, só parte delas pode retornar dados (outras dão 403). Veja os logs da extração.
# Pode ser sobrescrito pela Variable `football_data_org_competitions` (JSON array).
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

# Nome da pasta no data lake por competição (código -> pasta legível).
COMPETITION_FOLDER_NAMES = {
    "WC": "WC_FIFA_World_Cup",
    "CL": "CL_UEFA_Champions_League",
    "BL1": "BL1_Bundesliga",
    "DED": "DED_Eredivisie",
    "BSA": "BSA_Campeonato_Brasileiro",
    "PD": "PD_Primera_Division",
    "FL1": "FL1_Ligue_1",
    "ELC": "ELC_Championship",
    "PPL": "PPL_Primeira_Liga",
    "EC": "EC_European_Championship",
    "SA": "SA_Serie_A",
    "PL": "PL_Premier_League",
}

# Domain no path Bronze para partidas de competições (rota /competitions/.../matches).
DOMAIN_COMPETITION_MATCHES = "competition_matches"

# Connection ID no Airflow: API football-data.org (api.football-data.org/v4)
CONN_ID_FOOTBALL_DATA_ORG = "football_data_org"
