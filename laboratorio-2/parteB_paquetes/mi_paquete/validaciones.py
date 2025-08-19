"""Módulo de validaciones numéricas."""

def es_par(n: int) -> bool:
    return n % 2 == 0

# Importación relativa de operaciones
from .operaciones import sumar

def suma_si_par(a: int, b: int) -> int:
    """Suma dos números solo si ambos son pares."""
    if es_par(a) and es_par(b):
        return sumar(a, b)
    else:
        raise ValueError("Ambos números deben ser pares")
