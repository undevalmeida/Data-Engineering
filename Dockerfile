# Imagem base oficial Apache Airflow
FROM apache/airflow:2.9.0

USER root
RUN mkdir -p /opt/airflow/common /opt/airflow/pipelines /opt/airflow/dags /opt/airflow/plugins /opt/airflow/data \
    && chown -R airflow:root /opt/airflow
USER airflow

# Estrutura do data platform (compose monta a raiz em runtime)
COPY --chown=airflow:root common/ /opt/airflow/common/
COPY --chown=airflow:root pipelines/ /opt/airflow/pipelines/
COPY --chown=airflow:root dags/ /opt/airflow/dags/
COPY --chown=airflow:root plugins/ /opt/airflow/plugins/

ENV PYTHONPATH=/opt/airflow
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False

# Dependências extras (descomente se precisar)
# USER root
# RUN pip install --no-cache-dir some-package
# USER airflow
