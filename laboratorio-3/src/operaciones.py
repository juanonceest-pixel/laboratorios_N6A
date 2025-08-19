# src/operaciones.py
from typing import Union

def suma(a: Union[int, float], b: Union[int, float]) -> Union[int, float]:
    """Suma dos números."""
    return a + b

def division(a: Union[int, float], b: Union[int, float]) -> float:
    """Divide dos números. Lanza ZeroDivisionError si el divisor es 0."""
    if b == 0:
        raise ZeroDivisionError("No se puede dividir por cero")
    return float(a / b)

def multiplicar(a: int, b: int) -> int:
    """Multiplica dos enteros."""
    return a * b