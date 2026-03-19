# Operators customizados

from plugins.operators.bronze_load_operator import BronzeLoadOperator
from plugins.operators.parquet_transform_operator import ParquetTransformOperator
from plugins.operators.custom_operator_example import HelloOperator

__all__ = ["BronzeLoadOperator", "ParquetTransformOperator", "HelloOperator"]
