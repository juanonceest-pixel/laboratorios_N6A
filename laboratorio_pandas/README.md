# Laboratorio: Análisis Tabular con pandas y DuckDB

Este laboratorio compara dos enfoques para el análisis de datos tabulares: **pandas** (Python) y **DuckDB** (SQL embebido).

## Dataset
- **Fuente**: Niveles de reservorios de agua en Chennai, India
- **Archivo**: `chennai_reservoir_levels.csv`
- **Columnas**: Date, POONDI, CHOLAVARAM, REDHILLS, CHEMBARAMBAKKAM
- **Período**: 2004-2019 (aproximadamente)

## Estructura del Proyecto

# Análisis con pandas
python notebooks/01_pandas_analysis.py

# Análisis con DuckDB  
python notebooks/02_duckdb_analysis.py

# Comparación
python notebooks/03_comparison.py


# Verificar instalación
python -c "import pandas, duckdb; print('OK')"

# Ver archivos generados
ls -la outputs/

# Limpiar outputs
rm -rf outputs/*

# Reinstalar dependencias
pip install --upgrade -r requirements.txt