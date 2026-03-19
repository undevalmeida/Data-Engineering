# Football Data Engineering

Projeto de **engenharia de dados** e **orquestração com Apache Airflow** para ingestão de dados de futebol via **football-data.org (v4)**, com organização em camadas **Medallion (Bronze/Silver/Gold)**.

## Fontes de dados

O projeto usa a API **football-data.org** (`api.football-data.org/v4`). Connection no Airflow: `football_data_org`.

| Recurso | URL |
| :------ | :-- |
| Site | [football-data.org](https://www.football-data.org/coverage) |
| Documentação | [Quickstart](https://www.football-data.org/documentation/quickstart) |

Outra API de referência (apenas links): [api-football.com](https://www.api-football.com/), [Documentation v3](https://www.api-football.com/documentation-v3).

## Estrutura

```text
.
├── common/                  
│   ├── config.py
│   ├── constants.py
│   └── logging.py
├── dags/
│   ├── competition_matches/
│   │   └── competition_matches_football_data_org_dag.py
│   ├── players/
│   │   └── players_football_data_org_dag.py
│   └── teams/
│       └── teams_football_data_org_dag.py
├── pipelines/               
│   ├── matches/
│   │   ├── extract_matches_api.py
│   │   ├── load_matches_bronze.py
│   │   ├── transform_matches_silver.py
│   │   └── aggregate_matches_gold.py
│   ├── players/
│   │   ├── extract_players_api.py
│   │   ├── load_players_bronze.py
│   │   ├── transform_players_silver.py
│   │   └── aggregate_players_gold.py
│   └── teams/
│       ├── extract_teams_api.py
│       ├── load_teams_bronze.py
│       ├── transform_teams_silver.py
│       └── aggregate_teams_gold.py
├── plugins/
│   ├── hooks/
│   │   ├── football_data_org_hook.py   # API football-data.org
│   │   └── postgres_hook.py
│   ├── operators/
│   │   ├── bronze_load_operator.py
│   │   └── parquet_transform_operator.py
│   ├── sensors/
│   │   └── api_sensor.py
│   └── macros/
│       └── date_macros.py
├── data/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── sql/                     
├── models/                  
├── data_quality/            
├── tests/
│   ├── pipelines/
│   └── operators/
├── docker/
│   └── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── README.md
```

## Arquitetura Medallion

A pasta **`data/`** é persistida via Docker (montagem `.:/opt/airflow`); dentro dos containers fica em `/opt/airflow/data`. O projeto adota o modelo **Medallion** (Bronze → Silver → Gold): os dados passam por camadas com responsabilidades bem definidas, e as DAGs trocam informações apenas por arquivos nessa pasta para trafegar dados entre tasks.

### Convenções de paths (exemplo: partidas por competição)

- **Bronze (competition matches)**: `data/bronze/competition_matches/{PL_Premier_League}/{YYYY-MM-DD}/competition_matches_PL_2017.json`

| Camada | Papel | Conteúdo |
| :-- | :-- | :-- |
| **Bronze** | Ingestão bruta | Cópia fiel da resposta da fonte (ex.: JSON da API). Sem transformação; permite reprocessamento e auditoria. |
| **Silver** | Curated | Dados limpos, tipados e deduplicados. Estrutura padronizada, pronta para consumo downstream. |
| **Gold** | Analítica | Agregados e métricas de negócio (ex.: rankings, resumos por competição). Camada final para relatórios e dashboards. |

Fluxo: **fonte externa → Bronze** (extração) **→ Silver** (limpeza e modelagem) **→ Gold** (agregação). Cada etapa lê da camada anterior e grava na seguinte usando paths fixos em `data/`.

## Pré-requisitos

- [Docker](https://docs.docker.com/get-docker/)

## Licença

Este projeto está sob a licença MIT. Veja o arquivo [LICENSE](LICENSE) para detalhes.
