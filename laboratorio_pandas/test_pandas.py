import pandas as pd
import numpy as np
import os
from datetime import datetime

# Verificar instalación
print("🔍 Verificando instalación...")
try:
    import pandas as pd
    import duckdb
    print("✅ pandas y duckdb instalados correctamente")
except ImportError as e:
    print(f"❌ Error de importación: {e}")
    exit(1)

# Verificar estructura
print("📁 Verificando estructura...")
os.makedirs('data', exist_ok=True)
os.makedirs('outputs', exist_ok=True)

# Verificar dataset
dataset_path = 'data/chennai_reservoir_levels.csv'
if not os.path.exists(dataset_path):
    print(f"📥 Descargando dataset...")
    try:
        import urllib.request
        url = "https://raw.githubusercontent.com/MainakRepositor/Datasets/master/chennai_reservoir_levels.csv"
        urllib.request.urlretrieve(url, dataset_path)
        print(f"✅ Dataset descargado: {dataset_path}")
    except Exception as e:
        print(f"❌ Error descargando: {e}")
        print("🔧 Descarga manualmente desde: https://github.com/MainakRepositor/Datasets")
        exit(1)

# ANÁLISIS CON PANDAS
print("\n" + "="*50)
print("🐍 INICIANDO ANÁLISIS CON PANDAS")
print("="*50)

# Cargar dataset
df = pd.read_csv(dataset_path)
print(f"📊 Dataset cargado: {df.shape[0]} filas, {df.shape[1]} columnas")

# Exploración básica
print(f"\n--- Exploración básica ---")
print(f"Columnas: {list(df.columns)}")
print(f"Tipos de datos:\n{df.dtypes}")
print(f"Valores nulos:\n{df.isnull().sum()}")

# Mostrar primeras filas
print(f"\n--- Primeras 5 filas ---")
print(df.head())

# Crear columnas derivadas
print(f"\n--- Creando columnas derivadas ---")
df['Date'] = pd.to_datetime(df['Date'], format='%d-%m-%Y')
df['Total_Water'] = df['POONDI'] + df['CHOLAVARAM'] + df['REDHILLS'] + df['CHEMBARAMBAKKAM']
df['Year'] = df['Date'].dt.year
df['Month'] = df['Date'].dt.month

# Clasificación de nivel
def classify_water_level(total):
    if total < 100:
        return 'Crítico'
    elif total < 300:
        return 'Bajo'
    elif total < 600:
        return 'Normal'
    else:
        return 'Alto'

df['Water_Level_Category'] = df['Total_Water'].apply(classify_water_level)

print(f"✅ Columnas derivadas creadas")
print(f"Nuevas columnas: Total_Water, Year, Month, Water_Level_Category")

# Análisis por año
print(f"\n--- Análisis por año ---")
yearly_analysis = df.groupby('Year').agg({
    'Total_Water': ['mean', 'max', 'min'],
    'POONDI': 'mean',
    'REDHILLS': 'mean'
}).round(2)

yearly_analysis.columns = ['_'.join(col).strip() for col in yearly_analysis.columns]
yearly_analysis = yearly_analysis.reset_index()

print(yearly_analysis.head(10))

# Análisis por categoría
print(f"\n--- Análisis por categoría ---")
category_analysis = df['Water_Level_Category'].value_counts()
print(category_analysis)

# Exportar resultados
print(f"\n--- Exportando resultados ---")
yearly_analysis.to_csv('outputs/test_pandas_yearly.csv', index=False)
df.to_csv('outputs/test_pandas_complete.csv', index=False)

print(f"✅ Archivos exportados:")
print(f"   📄 outputs/test_pandas_yearly.csv")
print(f"   📄 outputs/test_pandas_complete.csv")

# Resumen
print(f"\n🎉 ANÁLISIS COMPLETADO")
print(f"📊 Registros procesados: {len(df):,}")
print(f"📅 Período: {df['Year'].min()} - {df['Year'].max()}")
print(f"💧 Promedio total de agua: {df['Total_Water'].mean():.2f}")

print(f"\n💡 Para continuar:")
print(f"   1. Revisa los archivos en 'outputs/'")
print(f"   2. Crea los demás scripts del laboratorio")
print(f"   3. Ejecuta: python run_lab.py")