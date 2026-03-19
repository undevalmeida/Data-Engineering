"""
Configuração de logging da plataforma.

Centraliza formato e nível para uso em pipelines e operators.
"""
import logging
import sys


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Retorna um logger configurado com formato padrão.

    Args:
        name: Nome do logger (geralmente __name__ do módulo).
        level: Nível de log (default INFO).

    Returns:
        Logger configurado.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
        )
        logger.addHandler(handler)
        logger.setLevel(level)
    return logger
