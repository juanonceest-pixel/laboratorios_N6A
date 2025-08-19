"""Módulo de operaciones aritméticas básicas."""

def sumar(a: int, b: int) -> int:
    return a + b

def dividir(a: int, b: int) -> float:
    if b == 0:
        raise ValueError("No se puede dividir entre cero")
    return a / b
