"""Módulo common: config, constantes e logging da plataforma."""

from common.config import get_data_base_path, get_bronze_path, get_silver_path, get_gold_path
from common.constants import COMPETITIONS

__all__ = [
    "get_data_base_path",
    "get_bronze_path",
    "get_silver_path",
    "get_gold_path",
    "COMPETITIONS",
]
