# tests/__init__.py
"""
Paquete de pruebas unitarias para el laboratorio de Pytest + CSV.
Contiene todas las pruebas para validar la funcionalidad de los módulos.
"""

import os
import sys

# Agregar el directorio src al path para importar módulos durante las pruebas
# Esto permite que las pruebas encuentren los módulos sin instalación
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
src_path = os.path.join(parent_dir, 'src')

if src_path not in sys.path:
    sys.path.insert(0, src_path)

# Configuración común para todas las pruebas
import pytest

# Fixtures globales que pueden ser usadas por todos los tests
@pytest.fixture(scope="session")
def sample_texts():
    """Fixture que proporciona textos de muestra para las pruebas."""
    return {
        "normal": "Hola Mundo desde Python",
        "mixed_case": "PyThOn Es GeNiAl",
        "with_spaces": "  texto con espacios  ",
        "empty": "",
        "only_spaces": "   ",
        "single_word": "Python"
    }

@pytest.fixture(scope="session")
def sample_emails():
    """Fixture que proporciona emails de muestra para las pruebas."""
    return {
        "valid": [
            "test@example.com",
            "usuario@dominio.co",
            "admin@test.org",
            "user.name@domain.com.co"
        ],
        "invalid": [
            "test",
            "@example.com", 
            "test@",
            "test@@example.com",
            "test@example",
            "",
            "test @example.com"
        ]
    }

# Configuración de logging para tests
import logging

# Configurar logging para las pruebas (nivel DEBUG para más detalle)
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Información del paquete de pruebas
__version__ = "3.13.3"
__author__ = "Sebastian Once"

print(f"Inicializando suite de pruebas v{__version__}")