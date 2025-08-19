# Laboratorio Python - Codespaces & Herramientas de Desarrollo

Un proyecto de Python que demuestra el uso de entornos virtuales, herramientas de calidad de código y pruebas automatizadas en GitHub Codespaces.

## ¿Qué hace este proyecto?

Implementa funciones matemáticas básicas con todas las mejores prácticas de desarrollo Python:
- Funciones de suma, división y multiplicación
- Tipado completo con mypy
- Code linting y formatting con ruff
- Pruebas automatizadas con pytest
- Todo desarrollado en GitHub Codespaces

## Estructura del proyecto

```
laboratorio-3/
├── src/
│   ├── __init__.py
│   └── operaciones.py      # Funciones matemáticas con tipado
├── tests/
│   ├── __init__.py
│   └── test_operaciones.py # Pruebas unitarias
├── main.py                 # Script principal para probar las funciones
├── pyproject.toml          # Configuración de ruff, mypy y pytest
├── requirements.txt        # Dependencias del proyecto
└── .venv/                  # Entorno virtual (no versionado)
```

## Instalación y configuración

### 1. Clonar y configurar entorno

```bash
# Crear entorno virtual
python -m venv .venv

# Activar entorno virtual
# En Linux/Mac:
source .venv/bin/activate
# En Windows:
.venv\Scripts\activate

# Instalar dependencias
pip install -r requirements.txt
```

### 2. Verificar que todo funciona

```bash
# Ejecutar el programa principal
python main.py

# Ejecutar todas las verificaciones
ruff check .                                    # Linting
mypy --explicit-package-bases src/ main.py     # Type checking
pytest                                          # Pruebas
```

## Qué incluye

### Funciones implementadas (`src/operaciones.py`)
- **suma()**: Suma dos números (int o float)
- **division()**: Divide dos números, maneja división por cero correctamente
- **multiplicar()**: Multiplica dos enteros

### Pruebas (`tests/test_operaciones.py`)
- Pruebas para casos normales y casos límite
- Manejo de excepciones (división por cero)
- 4 pruebas que cubren todas las funciones

## Herramientas configuradas

### Ruff (Linting y Formatting)
- Verifica el estilo del código
- Formatea automáticamente
- Configurado en `pyproject.toml`

```bash
ruff check .        # Verificar errores
ruff format .       # Formatear código
```

### MyPy (Type Checking)
- Verifica que los tipos sean correctos
- Modo estricto habilitado
- No permite funciones sin tipado

```bash
mypy --explicit-package-bases src/ main.py
```

### Pytest (Testing)
- Framework de pruebas moderno
- Auto-descubrimiento de tests
- Reportes claros

```bash
pytest              # Ejecutar todas las pruebas
pytest -v           # Verbose mode
```

## Comandos útiles

```bash
# Verificación completa del proyecto
ruff check . && mypy --explicit-package-bases src/ main.py && pytest

# Formatear todo el código
ruff format .

# Actualizar el archivo de dependencias exactas
pip freeze > requirements.lock

# Ejecutar solo las pruebas de una función específica
pytest tests/test_operaciones.py::test_suma
```

## Lo que aprendí en este laboratorio

- **Entornos virtuales**: Cómo aislar dependencias de cada proyecto
- **Codespaces**: Desarrollo en la nube sin configurar nada local
- **Type hints**: Escribir código más seguro con tipado
- **Herramientas modernas**: ruff (más rápido que flake8/black) y mypy
- **Testing**: Escribir pruebas que realmente verifican el comportamiento
- **Configuración**: Centralizar todo en `pyproject.toml`

## Tecnologías usadas

- **Python 3.9+**
- **ruff**: Linter y formateador ultra-rápido
- **mypy**: Verificador de tipos estático  
- **pytest**: Framework de testing
- **GitHub Codespaces**: Entorno de desarrollo

---

Desarrollado como parte del laboratorio de herramientas de desarrollo Python.