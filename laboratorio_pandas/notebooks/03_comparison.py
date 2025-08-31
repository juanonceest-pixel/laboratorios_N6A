import pandas as pd
import time
import os

print("=== COMPARACIÓN: pandas vs DuckDB ===")

# Comparar archivos generados
print("\n--- Archivos generados ---")
output_files = os.listdir('outputs')
print("Archivos en outputs/:")
for file in sorted(output_files):
    size = os.path.getsize(f'outputs/{file}')
    print(f"  {file}: {size:,} bytes")

# Comparar resultados pandas vs duckdb
print("\n--- Comparación de resultados ---")

# Cargar análisis anuales
pandas_yearly = pd.read_csv('outputs/pandas_yearly_analysis.csv')
duckdb_yearly = pd.read_csv('outputs/duckdb_yearly_analysis.csv')

print("Diferencias en análisis anual:")
print(f"Pandas shape: {pandas_yearly.shape}")
print(f"DuckDB shape: {duckdb_yearly.shape}")

# Verificar que los resultados sean similares
if 'Total_Water_mean' in pandas_yearly.columns and 'Total_Water_mean' in duckdb_yearly.columns:
    pandas_mean = pandas_yearly['Total_Water_mean'].mean()
    duckdb_mean = duckdb_yearly['Total_Water_mean'].mean()
    difference = abs(pandas_mean - duckdb_mean)
    print(f"Diferencia promedio en Total_Water_mean: {difference:.4f}")

print("\n--- Cuándo usar cada herramienta ---")
print("""
PANDAS:
✓ Mejor para análisis exploratorio interactivo
✓ Ideal para datasets que caben en memoria
✓ Gran ecosistema de visualización (matplotlib, seaborn)
✓ Sintaxis Python familiar
✓ Manipulación compleja de datos fila por fila
✓ Integración perfecta con Jupyter notebooks

DUCKDB:
✓ Mejor para datasets grandes (que no caben en memoria)
✓ Consultas SQL estándar - transferible a otros sistemas
✓ Rendimiento superior en agregaciones complejas
✓ Ideal para ETL y pipelines de datos
✓ Soporte nativo para múltiples formatos (Parquet, CSV, JSON)
✓ Menos uso de memoria RAM
✓ Mejor para análisis OLAP

RECOMENDACIONES:
- Dataset < 1GB + análisis exploratorio → pandas
- Dataset > 1GB + consultas complejas → DuckDB
- Prototipado rápido → pandas
- Producción y pipelines → DuckDB
- Equipos con SQL expertise → DuckDB
- Equipos con Python expertise → pandas
""")