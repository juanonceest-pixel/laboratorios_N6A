import pandas as pd
import requests
from io import StringIO

def explorar_datos_covid():
    """
    Paso 1: Exploración Manual de Datos (EDA)
    Genera tabla_perfilado.csv con estadísticas básicas
    """
    
    # Descargar datos desde OWID
    url = "https://catalog.ourworldindata.org/garden/covid/latest/compact/compact.csv"
    print("Descargando datos desde OWID...")
    response = requests.get(url)
    df = pd.read_csv(StringIO(response.text))
    
    print("=== COLUMNAS DISPONIBLES EN EL DATASET ===")
    print(df.columns.tolist())
    print(f"\nShape del dataset: {df.shape}")
    
    # Verificar qué columna contiene los países
    location_col = None
    possible_location_cols = ['location', 'country', 'entity', 'Country']
    for col in possible_location_cols:
        if col in df.columns:
            location_col = col
            break
    
    if location_col is None:
        print("ERROR: No se encontró columna de países")
        print("Columnas disponibles:", df.columns.tolist())
        print("Usando todo el dataset para el análisis...")
        df_filtrado = df
        paises_usados = "Todos los países disponibles"
    else:
        print(f"Usando columna '{location_col}' para países")
        
        # Filtrar por Ecuador y país comparativo (Perú)
        paises_interes = ['Ecuador', 'Peru']
        df_filtrado = df[df[location_col].isin(paises_interes)]
        
        print(f"\nPaíses únicos en el dataset: {df[location_col].unique()[:10]}...")  # Mostrar algunos
        print(f"Filas después del filtro: {len(df_filtrado)}")
        
        if len(df_filtrado) == 0:
            print("ADVERTENCIA: No se encontraron datos para Ecuador o Peru")
            print("Intentando con nombres alternativos...")
            # Buscar nombres similares
            unique_countries = df[location_col].unique()
            ecuador_matches = [c for c in unique_countries if 'ecuador' in c.lower()]
            peru_matches = [c for c in unique_countries if 'peru' in c.lower()]
            print(f"Posibles matches para Ecuador: {ecuador_matches}")
            print(f"Posibles matches para Peru: {peru_matches}")
            
            if len(ecuador_matches) > 0 or len(peru_matches) > 0:
                paises_encontrados = ecuador_matches + peru_matches
                df_filtrado = df[df[location_col].isin(paises_encontrados)]
                print(f"Usando países: {paises_encontrados}")
                paises_usados = ', '.join(paises_encontrados)
            else:
                print("No se pudieron encontrar datos para ningún país. Usando todo el dataset para el análisis.")
                df_filtrado = df
                paises_usados = "Todos los países disponibles"
        else:
            paises_usados = ', '.join(df_filtrado[location_col].unique())
    
    # Realizar perfilado básico
    perfilado = []
    
    # 1. Columnas y tipos de datos
    for col in df_filtrado.columns:
        tipo_dato = str(df_filtrado[col].dtype)
        perfilado.append({
            'metrica': f'tipo_dato_{col}',
            'valor': tipo_dato,
            'descripcion': f'Tipo de dato de la columna {col}'
        })
    
    # 2. Mínimo y máximo de new_cases (buscar columna)
    cases_col = None
    possible_cases_cols = ['new_cases', 'new_confirmed', 'daily_cases', 'cases']
    for col in possible_cases_cols:
        if col in df_filtrado.columns:
            cases_col = col
            break
    
    if cases_col:
        min_casos = df_filtrado[cases_col].min()
        max_casos = df_filtrado[cases_col].max()
        perfilado.append({
            'metrica': f'min_{cases_col}',
            'valor': min_casos,
            'descripcion': f'Valor mínimo de {cases_col}'
        })
        perfilado.append({
            'metrica': f'max_{cases_col}',
            'valor': max_casos,
            'descripcion': f'Valor máximo de {cases_col}'
        })
        
        # Porcentaje de valores faltantes en casos
        pct_faltantes_casos = (df_filtrado[cases_col].isnull().sum() / len(df_filtrado)) * 100
        perfilado.append({
            'metrica': f'pct_faltantes_{cases_col}',
            'valor': f"{pct_faltantes_casos:.2f}%",
            'descripcion': f'Porcentaje de valores faltantes en {cases_col}'
        })
    
    # 3. Porcentaje de valores faltantes en people_vaccinated (buscar columna)
    vac_col = None
    possible_vac_cols = ['people_vaccinated', 'total_vaccinations', 'vaccinated']
    for col in possible_vac_cols:
        if col in df_filtrado.columns:
            vac_col = col
            break
    
    if vac_col:
        pct_faltantes_vac = (df_filtrado[vac_col].isnull().sum() / len(df_filtrado)) * 100
        perfilado.append({
            'metrica': f'pct_faltantes_{vac_col}',
            'valor': f"{pct_faltantes_vac:.2f}%",
            'descripcion': f'Porcentaje de valores faltantes en {vac_col}'
        })
    
    # 4. Rango de fechas cubierto
    date_col = None
    possible_date_cols = ['date', 'Date', 'time', 'Time']
    for col in possible_date_cols:
        if col in df_filtrado.columns:
            date_col = col
            break
    
    if date_col:
        df_filtrado[date_col] = pd.to_datetime(df_filtrado[date_col])
        fecha_min = df_filtrado[date_col].min()
        fecha_max = df_filtrado[date_col].max()
        perfilado.append({
            'metrica': 'fecha_minima',
            'valor': fecha_min.strftime('%Y-%m-%d'),
            'descripcion': 'Fecha más antigua en el dataset'
        })
        perfilado.append({
            'metrica': 'fecha_maxima',
            'valor': fecha_max.strftime('%Y-%m-%d'),
            'descripcion': 'Fecha más reciente en el dataset'
        })
    
    # 5. Información adicional
    perfilado.append({
        'metrica': 'total_filas',
        'valor': len(df_filtrado),
        'descripcion': 'Total de filas en el dataset filtrado'
    })
    
    perfilado.append({
        'metrica': 'total_columnas',
        'valor': len(df_filtrado.columns),
        'descripcion': 'Total de columnas en el dataset'
    })
    
    perfilado.append({
        'metrica': 'paises_incluidos',
        'valor': paises_usados,
        'descripcion': 'Países incluidos en el análisis'
    })
    
    # Crear DataFrame del perfilado
    df_perfilado = pd.DataFrame(perfilado)
    
    # Guardar tabla de perfilado
    df_perfilado.to_csv('tabla_perfilado.csv', index=False)
    print("\n✅ Tabla de perfilado guardada como tabla_perfilado.csv")
    
    # Mostrar resumen
    print("\n=== RESUMEN DEL PERFILADO ===")
    for item in perfilado:
        print(f"{item['metrica']}: {item['valor']}")
    
    return df_perfilado

if __name__ == "__main__":
    explorar_datos_covid()