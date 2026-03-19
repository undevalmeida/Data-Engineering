"""
Hook para conexão com PostgreSQL da plataforma.

Encapsula o PostgresHook do Airflow com conn_id padrão da aplicação.
Requer uma Connection no Airflow (conn_type: postgres, host, schema, login, password, port).
Requer: apache-airflow-providers-postgres
"""
from airflow.providers.postgres.hooks.postgres import PostgresHook as _PostgresHook


class PostgresHook(_PostgresHook):
    """
    Hook PostgreSQL com conn_id padrão para o data platform.

    Uso: PostgresHook(conn_id="postgres_default") ou via Connection no Airflow.
    """

    default_conn_name = "postgres_default"
    conn_type = "postgres"
    hook_name = "Postgres (Data Platform)"
