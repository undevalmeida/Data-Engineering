"""
Operator que aplica transformação e pode persistir em Parquet.

Placeholder para evolução: hoje os pipelines usam JSON; quando houver
requisito de Parquet (Silver/Gold), este operator pode ler JSON e gravar Parquet
ou invocar lógica de transformação que gera Parquet.
"""
from airflow.models import BaseOperator


class ParquetTransformOperator(BaseOperator):
    """
    Operador que transforma dados (ex.: Silver) e grava em Parquet.

    Template fields: execution_ds, domain, layer.
    Requer pyarrow ou fastparquet para escrita Parquet.
    """

    template_fields = ("execution_ds", "domain", "layer")

    def __init__(
        self,
        domain: str,
        layer: str = "silver",
        execution_ds: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.domain = domain
        self.layer = layer
        self.execution_ds = execution_ds

    def execute(self, context):
        """Por enquanto apenas log; implementar leitura JSON → escrita Parquet quando necessário."""
        ds = self.execution_ds or context["ds"]
        self.log.info(
            "ParquetTransformOperator: domain=%s layer=%s ds=%s (placeholder)",
            self.domain,
            self.layer,
            ds,
        )
        # Exemplo de evolução: ler common.config get_silver_path(domain, ds), json.load, pd.DataFrame, to_parquet
        return {"domain": self.domain, "layer": self.layer, "ds": ds}
