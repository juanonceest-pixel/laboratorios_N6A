import pandas as pd
import requests
from datetime import datetime, timedelta
from dagster import asset, asset_check, AssetCheckResult, AssetCheckSeverity
import openpyxl
from typing import Dict, Any

# Paso 2 - Lectura de Datos
@asset
def leer_datos() -> pd.DataFrame:
    """
    Lee los datos COVID-19 desde la URL canónica de OWID.
    Sin transformaciones, solo lectura.
    """
    url = "https://catalog.ourworldindata.org/garden/covid/latest/compact/compact.csv"
    response = requests.get(url)
    
    # Crear DataFrame desde el contenido CSV
    from io import StringIO
    df = pd.read_csv(StringIO(response.text))
    
    return df

# Chequeos de Entrada
@asset_check(asset=leer_datos)
def check_fechas_futuras(leer_datos: pd.DataFrame) -> AssetCheckResult:
    """Verifica que no existan fechas futuras en los datos."""
    df = leer_datos
    df['date'] = pd.to_datetime(df['date'])
    fechas_futuras = df[df['date'] > datetime.now()]
    
    passed = len(fechas_futuras) == 0
    
    return AssetCheckResult(
        passed=passed,
        metadata={
            "filas_afectadas": len(fechas_futuras),
            "notas": "Verificación de fechas futuras en el dataset"
        }
    )

@asset_check(asset=leer_datos)
def check_columnas_clave(leer_datos: pd.DataFrame) -> AssetCheckResult:
    """Verifica que las columnas clave no tengan valores nulos."""
    df = leer_datos
    columnas_clave = ['location', 'date', 'population']
    
    nulos_por_columna = {}
    for col in columnas_clave:
        if col in df.columns:
            nulos_por_columna[col] = df[col].isnull().sum()
    
    total_nulos = sum(nulos_por_columna.values())
    passed = total_nulos == 0
    
    return AssetCheckResult(
        passed=passed,
        metadata={
            "filas_afectadas": total_nulos,
            "nulos_por_columna": nulos_por_columna,
            "notas": "Verificación de valores nulos en columnas clave"
        }
    )

# Paso 3 - Procesamiento de Datos
@asset
def datos_procesados(leer_datos: pd.DataFrame) -> pd.DataFrame:
    """
    Procesa los datos aplicando filtros y limpieza.
    """
    df = leer_datos.copy()
    
    # Eliminar filas con valores nulos en new_cases o people_vaccinated
    df_clean = df.dropna(subset=['new_cases', 'people_vaccinated'])
    
    # Eliminar duplicados basados en location y date
    df_clean = df_clean.drop_duplicates(subset=['location', 'date'])
    
    # Filtrar a Ecuador y país comparativo (Perú)
    paises_interes = ['Ecuador', 'Peru']
    df_filtrado = df_clean[df_clean['location'].isin(paises_interes)]
    
    # Seleccionar columnas esenciales
    columnas_esenciales = ['location', 'date', 'new_cases', 'people_vaccinated', 'population']
    df_final = df_filtrado[columnas_esenciales].copy()
    
    # Convertir date a datetime
    df_final['date'] = pd.to_datetime(df_final['date'])
    
    return df_final

# Paso 4A - Métrica de Incidencia a 7 días
@asset
def metrica_incidencia_7d(datos_procesados: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula la incidencia acumulada a 7 días por 100 mil habitantes.
    """
    df = datos_procesados.copy()
    df = df.sort_values(['location', 'date'])
    
    # Calcular incidencia diaria por 100k habitantes
    df['incidencia_diaria'] = (df['new_cases'] / df['population']) * 100000
    
    # Calcular promedio móvil de 7 días
    df['incidencia_7d'] = df.groupby('location')['incidencia_diaria'].rolling(
        window=7, min_periods=7
    ).mean().reset_index(level=0, drop=True)
    
    # Seleccionar columnas finales
    resultado = df[['date', 'location', 'incidencia_7d']].dropna()
    resultado.columns = ['fecha', 'país', 'incidencia_7d']
    
    return resultado

# Paso 4B - Factor de Crecimiento Semanal
@asset
def metrica_factor_crec_7d(datos_procesados: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula el factor de crecimiento semanal de casos.
    """
    df = datos_procesados.copy()
    df = df.sort_values(['location', 'date'])
    
    resultados = []
    
    for pais in df['location'].unique():
        df_pais = df[df['location'] == pais].copy()
        df_pais = df_pais.sort_values('date')
        
        for i in range(14, len(df_pais)):  # Necesitamos al menos 14 días
            fecha_fin = df_pais.iloc[i]['date']
            
            # Casos semana actual (últimos 7 días)
            casos_semana_actual = df_pais.iloc[i-6:i+1]['new_cases'].sum()
            
            # Casos semana previa (días 7-13 atrás)
            casos_semana_prev = df_pais.iloc[i-13:i-6]['new_cases'].sum()
            
            if casos_semana_prev > 0:
                factor_crec = casos_semana_actual / casos_semana_prev
            else:
                factor_crec = None
            
            if factor_crec is not None:
                resultados.append({
                    'semana_fin': fecha_fin,
                    'país': pais,
                    'casos_semana': casos_semana_actual,
                    'factor_crec_7d': factor_crec
                })
    
    return pd.DataFrame(resultados)

# Paso 5 - Chequeos de Salida
@asset_check(asset=metrica_incidencia_7d)
def check_incidencia_rango(metrica_incidencia_7d: pd.DataFrame) -> AssetCheckResult:
    """Valida que incidencia_7d esté en rango esperado (0-2000)."""
    df = metrica_incidencia_7d
    
    fuera_rango = df[(df['incidencia_7d'] < 0) | (df['incidencia_7d'] > 2000)]
    passed = len(fuera_rango) == 0
    
    return AssetCheckResult(
        passed=passed,
        metadata={
            "filas_afectadas": len(fuera_rango),
            "valor_min": df['incidencia_7d'].min(),
            "valor_max": df['incidencia_7d'].max(),
            "notas": "Validación de rango 0-2000 para incidencia_7d"
        }
    )

# Paso 6 - Exportación de Resultados
@asset
def reporte_excel_covid(
    datos_procesados: pd.DataFrame,
    metrica_incidencia_7d: pd.DataFrame,
    metrica_factor_crec_7d: pd.DataFrame
) -> str:
    """
    Exporta los resultados finales a un archivo Excel.
    """
    archivo_excel = "reporte_covid_resultados.xlsx"
    
    with pd.ExcelWriter(archivo_excel, engine='openpyxl') as writer:
        # Hoja 1: Datos procesados
        datos_procesados.to_excel(writer, sheet_name='Datos_Procesados', index=False)
        
        # Hoja 2: Métrica incidencia 7d
        metrica_incidencia_7d.to_excel(writer, sheet_name='Incidencia_7d', index=False)
        
        # Hoja 3: Métrica factor crecimiento 7d
        metrica_factor_crec_7d.to_excel(writer, sheet_name='Factor_Crec_7d', index=False)
    
    return f"Reporte exportado exitosamente a {archivo_excel}"