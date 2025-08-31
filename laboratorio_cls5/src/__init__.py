
"""
Paquete principal del laboratorio de Pytest + CSV.
Contiene módulos para procesamiento de texto y validación de datasets.
"""

# Importar las funciones principales para facilitar su uso
from .text_utils import clean_text, count_words, is_valid_email
from .csv_validator import CrimeDataValidator

# Información del paquete
__version__ = "3.13.3"
__author__ = "Sebastian Once"
__email__ = "juan.once.est@tecazuay.edu.ec"

# Lista de elementos públicos del módulo
__all__ = [
    "clean_text",
    "count_words", 
    "is_valid_email",
    "CrimeDataValidator"
]

# Configuración opcional para logging
import logging

# Crear logger para el paquete
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Si no hay handlers configurados, agregar uno básico
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)