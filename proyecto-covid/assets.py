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
    
    print(f"Dataset cargado con {len(df)} filas y {len(df.columns)} columnas")
    print(f"Columnas disponibles: {df.columns.tolist()}")
    
    return df

# Chequeos de Entrada
@asset_check(asset=leer_datos)
def check_fechas_futuras(leer_datos: pd.DataFrame) -> AssetCheckResult:
    """Verifica que no existan fechas futuras en los datos."""
    df = leer_datos
    
    # Buscar columna de fecha
    date_col = None
    for col in ['date', 'Date', 'time', 'Time']:
        if col in df.columns:
            date_col = col
            break
    
    if not date_col:
        return AssetCheckResult(
            passed=False,
            metadata={
                "error": "No se encontró columna de fecha",
                "columnas_disponibles": list(df.columns),
                "notas": "No se pudo realizar validación de fechas"
            }
        )
    
    df[date_col] = pd.to_datetime(df[date_col])
    fechas_futuras = df[df[date_col] > datetime.now()]
    
    passed = len(fechas_futuras) == 0
    
    return AssetCheckResult(
        passed=passed,
        metadata={
            "filas_afectadas": len(fechas_futuras),
            "columna_fecha_usada": date_col,
            "notas": "Verificación de fechas futuras en el dataset"
        }
    )

@asset_check(asset=leer_datos)
def check_columnas_clave(leer_datos: pd.DataFrame) -> AssetCheckResult:
    """Verifica que las columnas clave no tengan valores nulos."""
    df = leer_datos
    
    # Mapear columnas clave a nombres posibles
    columnas_mapeo = {
        'location': ['location', 'country', 'entity', 'Country'],
        'date': ['date', 'Date', 'time', 'Time'],
        'population': ['population', 'Population', 'pop']
    }
    
    columnas_encontradas = {}
    nulos_por_columna = {}
    
    for clave, posibles in columnas_mapeo.items():
        encontrada = None
        for col in posibles:
            if col in df.columns:
                encontrada = col
                break
        
        if encontrada:
            columnas_encontradas[clave] = encontrada
            nulos_por_columna[encontrada] = df[encontrada].isnull().sum()
        else:
            nulos_por_columna[f"{clave}_NO_ENCONTRADA"] = -1
    
    total_nulos = sum(v for v in nulos_por_columna.values() if v >= 0)
    passed = total_nulos == 0 and len(columnas_encontradas) == 3
    
    return AssetCheckResult(
        passed=passed,
        metadata={
            "filas_afectadas": total_nulos,
            "nulos_por_columna": nulos_por_columna,
            "columnas_encontradas": columnas_encontradas,
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
    
    # Mapear columnas a nombres posibles
    columnas_mapeo = {
        'location': ['location', 'country', 'entity', 'Country'],
        'date': ['date', 'Date', 'time', 'Time'],
        'new_cases': ['new_cases', 'new_confirmed', 'daily_cases'],
        'people_vaccinated': ['people_vaccinated', 'total_vaccinations', 'vaccinated'],
        'population': ['population', 'Population', 'pop']
    }
    
    # Encontrar columnas reales
    columnas_reales = {}
    for clave, posibles in columnas_mapeo.items():
        for col in posibles:
            if col in df.columns:
                columnas_reales[clave] = col
                break
    
    print(f"Columnas encontradas: {columnas_reales}")
    
    # Verificar columnas mínimas necesarias
    if 'location' not in columnas_reales or 'date' not in columnas_reales:
        raise ValueError(f"No se encontraron columnas esenciales. Disponibles: {list(df.columns)}")
    
    # Eliminar filas con valores nulos en columnas críticas si existen
    columnas_criticas = []
    if 'new_cases' in columnas_reales:
        columnas_criticas.append(columnas_reales['new_cases'])
    if 'people_vaccinated' in columnas_reales:
        columnas_criticas.append(columnas_reales['people_vaccinated'])
    
    if columnas_criticas:
        df_clean = df.dropna(subset=columnas_criticas)
        print(f"Filas después de eliminar nulos: {len(df_clean)}")
    else:
        df_clean = df.copy()
        print("No se encontraron columnas de casos o vacunación para limpiar")
    
    # Eliminar duplicados basados en location y date
    df_clean = df_clean.drop_duplicates(subset=[columnas_reales['location'], columnas_reales['date']])
    
    # Filtrar a Ecuador y país comparativo (Perú)
    paises_interes = ['Ecuador', 'Peru']
    location_col = columnas_reales['location']
    
    # Buscar países que coincidan (case insensitive)
    paises_disponibles = df_clean[location_col].unique()
    paises_encontrados = []
    
    for pais_interes in paises_interes:
        for pais_disponible in paises_disponibles:
            if pais_interes.lower() in pais_disponible.lower():
                paises_encontrados.append(pais_disponible)
                break
    
    if not paises_encontrados:
        print(f"ADVERTENCIA: No se encontraron países de interés. Usando los primeros 2 países disponibles.")
        print(f"Países disponibles: {paises_disponibles[:10]}")
        paises_encontrados = paises_disponibles[:2]
    
    print(f"Usando países: {paises_encontrados}")
    df_filtrado = df_clean[df_clean[location_col].isin(paises_encontrados)]
    
    # Seleccionar columnas esenciales que existen
    columnas_finales = [columnas_reales['location'], columnas_reales['date']]
    
    if 'new_cases' in columnas_reales:
        columnas_finales.append(columnas_reales['new_cases'])
    if 'people_vaccinated' in columnas_reales:
        columnas_finales.append(columnas_reales['people_vaccinated'])
    if 'population' in columnas_reales:
        columnas_finales.append(columnas_reales['population'])
    
    df_final = df_filtrado[columnas_finales].copy()
    
    # Estandarizar nombres de columnas
    rename_dict = {v: k for k, v in columnas_reales.items() if v in columnas_finales}
    df_final = df_final.rename(columns=rename_dict)
    
    # Convertir date a datetime
    df_final['date'] = pd.to_datetime(df_final['date'])
    
    print(f"Dataset final: {len(df_final)} filas, columnas: {list(df_final.columns)}")
    
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