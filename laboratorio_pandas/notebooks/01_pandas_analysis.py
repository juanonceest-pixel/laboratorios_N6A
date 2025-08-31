import pandas as pd
import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt

# A.1 - Lectura y exploración del dataset
print("=== PARTE A.1: LECTURA Y EXPLORACIÓN ===")

# Cargar el CSV
df = pd.read_csv('data/chennai_reservoir_levels.csv')

# Inspeccionar estructura básica
print(f"Dimensiones del dataset: {df.shape}")
print(f"Filas: {df.shape[0]}, Columnas: {df.shape[1]}")

# Mostrar primeras filas
print("\n--- Primeras 5 filas ---")
print(df.head())

# Información general
print("\n--- Información del DataFrame ---")
print(df.info())

# Tipos de datos
print("\n--- Tipos de datos ---")
print(df.dtypes)

# Conteo de nulos
print("\n--- Valores nulos por columna ---")
print(df.isnull().sum())

# Estadísticas descriptivas
print("\n--- Estadísticas descriptivas ---")
print(df.describe())

# Identificar columna clave: Date
print(f"\n--- Análisis de la columna clave 'Date' ---")
print(f"Rango de fechas: {df['Date'].min()} a {df['Date'].max()}")
print(f"Fechas únicas: {df['Date'].nunique()}")

# A.2 - Columnas derivadas y limpieza//////////////////////////////////////////////////////

print("\n\n=== PARTE A.2: COLUMNAS DERIVADAS Y LIMPIEZA ===")

# Convertir Date a datetime
df['Date'] = pd.to_datetime(df['Date'], format='%d-%m-%Y')

# Crear columnas derivadas
print("\n--- Creando columnas derivadas ---")

# 1. Total de agua almacenada por fecha
df['Total_Water'] = df['POONDI'] + df['CHOLAVARAM'] + df['REDHILLS'] + df['CHEMBARAMBAKKAM']

# 2. Año y mes
df['Year'] = df['Date'].dt.year
df['Month'] = df['Date'].dt.month
df['Month_Name'] = df['Date'].dt.month_name()

# 3. Promedio diario de reservorios
df['Average_Level'] = df[['POONDI', 'CHOLAVARAM', 'REDHILLS', 'CHEMBARAMBAKKAM']].mean(axis=1)

# 4. Clasificación de nivel de agua
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

# 5. Reservorio dominante (con más agua)
reservoirs = ['POONDI', 'CHOLAVARAM', 'REDHILLS', 'CHEMBARAMBAKKAM']
df['Dominant_Reservoir'] = df[reservoirs].idxmax(axis=1)

# Gestión de nulos
print(f"\n--- Gestión de valores nulos ---")
print("Nulos antes de la limpieza:")
print(df.isnull().sum())

# Imputar nulos con la mediana (si los hay)
for col in reservoirs:
    if df[col].isnull().sum() > 0:
        df[col].fillna(df[col].median(), inplace=True)

print("Nulos después de la limpieza:")
print(df.isnull().sum())

# Mostrar nuevas columnas
print(f"\n--- Nuevas columnas creadas ---")
new_columns = ['Total_Water', 'Year', 'Month', 'Month_Name', 'Average_Level', 
               'Water_Level_Category', 'Dominant_Reservoir']
print(df[['Date'] + new_columns].head())

# Guardar el DataFrame limpio///////////////////////////
print("\n\n=== PARTE A.3: AGRUPACIONES Y FILTROS ===")

# Filtros booleanos
print("\n--- Aplicando filtros ---")

# Filtrar años después de 2010
df_recent = df[df['Year'] >= 2010].copy()
print(f"Registros después de 2010: {len(df_recent)}")

# Filtrar solo niveles críticos y bajos
df_low_water = df[df['Water_Level_Category'].isin(['Crítico', 'Bajo'])].copy()
print(f"Registros con niveles críticos/bajos: {len(df_low_water)}")

# Agrupaciones y métricas
print("\n--- Análisis por año ---")
yearly_analysis = df.groupby('Year').agg({
    'Total_Water': ['mean', 'max', 'min', 'std'],
    'Average_Level': 'mean',
    'POONDI': 'mean',
    'CHOLAVARAM': 'mean',
    'REDHILLS': 'mean',
    'CHEMBARAMBAKKAM': 'mean'
}).round(2)

# Aplanar nombres de columnas
yearly_analysis.columns = ['_'.join(col).strip() for col in yearly_analysis.columns]
yearly_analysis = yearly_analysis.reset_index()

print(yearly_analysis.head(10))

print("\n--- Análisis por mes ---")
monthly_analysis = df.groupby(['Month', 'Month_Name']).agg({
    'Total_Water': ['mean', 'max', 'min'],
    'Water_Level_Category': lambda x: x.mode()[0] if not x.empty else 'Normal',
    'Dominant_Reservoir': lambda x: x.mode()[0] if not x.empty else 'REDHILLS'
}).round(2)

monthly_analysis.columns = ['_'.join(col).strip() if col[1] else col[0] for col in monthly_analysis.columns]
monthly_analysis = monthly_analysis.reset_index()

print(monthly_analysis)

print("\n--- Análisis por categoría de nivel de agua ---")
category_analysis = df.groupby('Water_Level_Category').agg({
    'Date': 'count',
    'Total_Water': 'mean',
    'Year': ['min', 'max']
}).round(2)

category_analysis.columns = ['_'.join(col).strip() for col in category_analysis.columns]
category_analysis = category_analysis.reset_index()

print(category_analysis)

# Exportar resultados
print("\n--- Exportando resultados ---")

# Crear directorio outputs si no existe
import os
os.makedirs('outputs', exist_ok=True)

# Exportar análisis anual a CSV
yearly_analysis.to_csv('outputs/pandas_yearly_analysis.csv', index=False)
print("✓ Análisis anual exportado a: outputs/pandas_yearly_analysis.csv")

# Exportar análisis mensual a CSV
monthly_analysis.to_csv('outputs/pandas_monthly_analysis.csv', index=False)
print("✓ Análisis mensual exportado a: outputs/pandas_monthly_analysis.csv")

# Exportar dataset completo con nuevas columnas a Parquet
df.to_parquet('outputs/pandas_complete_dataset.parquet', index=False)
print("✓ Dataset completo exportado a: outputs/pandas_complete_dataset.parquet")

print(f"\n--- Resumen final ---")
print(f"Total de registros procesados: {len(df)}")
print(f"Rango temporal: {df['Date'].min().strftime('%Y-%m-%d')} a {df['Date'].max().strftime('%Y-%m-%d')}")
print(f"Columnas originales: 5")
print(f"Columnas después del procesamiento: {len(df.columns)}")