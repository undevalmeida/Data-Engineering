"""
Exemplo de Operator customizado.
Use como base para criar operators específicos do seu domínio.
"""
from airflow.models import BaseOperator


class HelloOperator(BaseOperator):
    """Operator que executa uma saudação configurável."""

    template_fields = ("name",)

    def __init__(self, name: str = "World", **kwargs):
        super().__init__(**kwargs)
        self.name = name

    def execute(self, context):
        msg = f"Hello from custom operator, {self.name}!"
        self.log.info(msg)
        return msg
