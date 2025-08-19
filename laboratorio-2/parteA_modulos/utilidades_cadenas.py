"""
Módulo: utilidades_cadenas
Funciones útiles para normalizar, validar y formatear cadenas de texto.
"""

def normalizar(cadena: str) -> str:
    """Convierte una cadena a minúsculas y elimina espacios en los extremos."""
    return cadena.strip().lower()

def es_palabra_valida(cadena: str) -> bool:
    """Verifica que una cadena solo contenga letras y espacios."""
    return cadena.replace(" ", "").isalpha()

def formatear_titulo(cadena: str) -> str:
    """Convierte la cadena en formato título (cada palabra con mayúscula inicial)."""
    return cadena.title()

def obtener_longitud(cadena: str) -> int:
    """Devuelve la longitud de la cadena."""
    return len(cadena)
