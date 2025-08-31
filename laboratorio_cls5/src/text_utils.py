"""
Módulo de utilidades para procesamiento de texto.
Contiene funciones para validación, limpieza y análisis básico de cadenas.
"""

def clean_text(text: str) -> str:
    """
    Limpia un texto eliminando espacios extra y convirtiendo a minúsculas.
    
    Args:
        text (str): Texto a limpiar
        
    Returns:
        str: Texto limpio
        
    Raises:
        TypeError: Si el input no es una cadena
    """
    if not isinstance(text, str):
        raise TypeError("El parámetro debe ser una cadena de texto")
    
    return text.strip().lower()

def count_words(text: str) -> int:
    """
    Cuenta el número de palabras en un texto.
    
    Args:
        text (str): Texto a analizar
        
    Returns:
        int: Número de palabras
        
    Raises:
        TypeError: Si el input no es una cadena
    """
    if not isinstance(text, str):
        raise TypeError("El parámetro debe ser una cadena de texto")
    
    if not text.strip():
        return 0
    
    return len(text.split())

def is_valid_email(email: str) -> bool:
    """
    Valida si una cadena tiene formato básico de email.
    
    Args:
        email (str): Email a validar
        
    Returns:
        bool: True si es válido, False en caso contrario
        
    Raises:
        TypeError: Si el input no es una cadena
    """
    if not isinstance(email, str):
        raise TypeError("El parámetro debe ser una cadena de texto")
    
    email = email.strip()
    
    # Validación básica: debe contener @ y al menos un punto después del @
    if '@' not in email:
        return False
    
    parts = email.split('@')
    if len(parts) != 2:
        return False
    
    local, domain = parts
    
    # El local y dominio no pueden estar vacíos
    if not local or not domain:
        return False
    
    # El dominio debe tener al menos un punto
    if '.' not in domain:
        return False
    
    return True