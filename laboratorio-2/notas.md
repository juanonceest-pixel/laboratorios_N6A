# Laboratorio 2 — Módulos, Paquetes y Anotaciones de Tipado

## Parte A — Módulos
- **API pública (`utilidades_cadenas.py`)**:
  - `normalizar(cadena)` → Normaliza texto.
  - `es_palabra_valida(cadena)` → Valida que solo contenga letras.
  - `formatear_titulo(cadena)` → Da formato tipo título.
  - `obtener_longitud(cadena)` → Longitud del texto.
- Separé este módulo porque las funciones son coherentes (todas trabajan sobre cadenas).

## Parte B — Paquetes
- **Cuándo usar importaciones absolutas:** en el script principal (`import mi_paquete...`).
- **Cuándo usar relativas:** dentro de un mismo paquete (ej. `from .operaciones import sumar`).
- El archivo `__init__.py` expone solo las funciones clave para simplificar el uso.

## Parte C — Anotaciones de tipado
- **Qué aportan:** claridad en el código, ayudan a detectar errores con herramientas estáticas (mypy, pyright).
- **Limitaciones:** Python no obliga a cumplir tipos en tiempo de ejecución (solo son sugerencias).
- **Ejemplo de uso:**
  ```python
  from typing import Union, Optional

  def procesar_dato(x: Union[int, str], factor: Optional[int] = None) -> str:
      return str(x) * (factor if factor else 1)
