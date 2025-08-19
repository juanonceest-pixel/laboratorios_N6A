# tests/test_operaciones.py
import pytest
from src.operaciones import suma, division, multiplicar

def test_suma():
    assert suma(2, 3) == 5
    assert suma(2.5, 3.5) == 6.0
    assert suma(-1, 1) == 0

def test_division():
    assert division(10, 2) == 5.0
    assert division(7, 2) == 3.5
    
def test_division_por_cero():
    with pytest.raises(ZeroDivisionError):
        division(10, 0)

def test_multiplicar():
    assert multiplicar(3, 4) == 12
    assert multiplicar(0, 5) == 0
    assert multiplicar(-2, 3) == -6