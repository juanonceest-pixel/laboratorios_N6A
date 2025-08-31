import duckdb
import pandas as pd
from datetime import datetime

# B.1 - Primer query sobre archivo
print("=== PARTE B.1: PRIMER QUERY CON DUCKDB ===")

# Conectar a DuckDB
conn = duckdb.connect()

# Query básico: leer directamente el CSV
print("\n--- Consulta básica del CSV ---")
basic_query = """
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT Date) as unique_dates,
    MIN(Date) as earliest_date,
    MAX(Date) as latest_date
FROM 'data/chennai_reservoir_levels.csv'
"""

result = conn.execute(basic_query).fetchall()
print("Estadísticas básicas:")
print(f"Total registros: {result[0][0]}")
print(f"Fechas únicas: {result[0][1]}")
print(f"Fecha más antigua: {result[0][2]}")
print(f"Fecha más reciente: {result[0][3]}")

# Query con selección de columnas y filtros
print("\n--- Query con filtros ---")
filtered_query = """
SELECT 
    Date,
    POONDI,
    REDHILLS,
    (POONDI + CHOLAVARAM + REDHILLS + CHEMBARAMBAKKAM) as Total_Water
FROM 'data/chennai_reservoir_levels.csv'
WHERE POONDI > 50 OR REDHILLS > 200
ORDER BY Date DESC
LIMIT 10
"""

filtered_result = conn.execute(filtered_query).df()
print("Primeros 10 registros con filtro aplicado:")
print(filtered_result)

# Conteo por condiciones
print("\n--- Conteos condicionales ---")
count_query = """
SELECT 
    COUNT(CASE WHEN POONDI = 0 THEN 1 END) as poondi_empty,
    COUNT(CASE WHEN CHOLAVARAM = 0 THEN 1 END) as cholavaram_empty,
    COUNT(CASE WHEN REDHILLS = 0 THEN 1 END) as redhills_empty,
    COUNT(CASE WHEN CHEMBARAMBAKKAM = 0 THEN 1 END) as chembarambakkam_empty,
    COUNT(*) as total_records
FROM 'data/chennai_reservoir_levels.csv'
"""

count_result = conn.execute(count_query).fetchall()
print("Días con reservorios vacíos:")
for i, reservoir in enumerate(['POONDI', 'CHOLAVARAM', 'REDHILLS', 'CHEMBARAMBAKKAM']):
    print(f"{reservoir}: {count_result[0][i]} días vacíos")

#análisis en SQL///////////////////////////////////

print("\n\n=== PARTE B.2: ANÁLISIS COMPLETO CON DUCKDB ===")

# Query complejo replicando el análisis de pandas
print("\n--- Análisis completo con columnas derivadas ---")
complete_analysis_query = """
WITH processed_data AS (
    SELECT 
        strptime(Date, '%d-%m-%Y') as parsed_date,
        Date,
        POONDI,
        CHOLAVARAM, 
        REDHILLS,
        CHEMBARAMBAKKAM,
        -- Columnas derivadas
        (POONDI + CHOLAVARAM + REDHILLS + CHEMBARAMBAKKAM) as Total_Water,
        EXTRACT(year FROM strptime(Date, '%d-%m-%Y')) as Year,
        EXTRACT(month FROM strptime(Date, '%d-%m-%Y')) as Month,
        (POONDI + CHOLAVARAM + REDHILLS + CHEMBARAMBAKKAM) / 4.0 as Average_Level,
        -- Clasificación de nivel de agua
        CASE 
            WHEN (POONDI + CHOLAVARAM + REDHILLS + CHEMBARAMBAKKAM) < 100 THEN 'Crítico'
            WHEN (POONDI + CHOLAVARAM + REDHILLS + CHEMBARAMBAKKAM) < 300 THEN 'Bajo'
            WHEN (POONDI + CHOLAVARAM + REDHILLS + CHEMBARAMBAKKAM) < 600 THEN 'Normal'
            ELSE 'Alto'
        END as Water_Level_Category,
        -- Reservorio dominante
        CASE 
            WHEN POONDI >= CHOLAVARAM AND POONDI >= REDHILLS AND POONDI >= CHEMBARAMBAKKAM THEN 'POONDI'
            WHEN CHOLAVARAM >= POONDI AND CHOLAVARAM >= REDHILLS AND CHOLAVARAM >= CHEMBARAMBAKKAM THEN 'CHOLAVARAM'
            WHEN REDHILLS >= POONDI AND REDHILLS >= CHOLAVARAM AND REDHILLS >= CHEMBARAMBAKKAM THEN 'REDHILLS'
            ELSE 'CHEMBARAMBAKKAM'
        END as Dominant_Reservoir
    FROM 'data/chennai_reservoir_levels.csv'
)
SELECT * FROM processed_data
ORDER BY parsed_date
LIMIT 10
"""

complete_result = conn.execute(complete_analysis_query).df()
print("Dataset procesado con columnas derivadas:")
print(complete_result)

# Análisis anual (equivalente al groupby de pandas)
print("\n--- Análisis por año ---")
yearly_sql = """
WITH processed_data AS (
    SELECT 
        EXTRACT(year FROM strptime(Date, '%d-%m-%Y')) as Year,
        (POONDI + CHOLAVARAM + REDHILLS + CHEMBARAMBAKKAM) as Total_Water,
        (POONDI + CHOLAVARAM + REDHILLS + CHEMBARAMBAKKAM) / 4.0 as Average_Level,
        POONDI, CHOLAVARAM, REDHILLS, CHEMBARAMBAKKAM
    FROM 'data/chennai_reservoir_levels.csv'
)
SELECT 
    Year,
    ROUND(AVG(Total_Water), 2) as Total_Water_mean,
    ROUND(MAX(Total_Water), 2) as Total_Water_max,
    ROUND(MIN(Total_Water), 2) as Total_Water_min,
    ROUND(STDDEV(Total_Water), 2) as Total_Water_std,
    ROUND(AVG(Average_Level), 2) as Average_Level_mean,
    ROUND(AVG(POONDI), 2) as POONDI_mean,
    ROUND(AVG(CHOLAVARAM), 2) as CHOLAVARAM_mean,
    ROUND(AVG(REDHILLS), 2) as REDHILLS_mean,
    ROUND(AVG(CHEMBARAMBAKKAM), 2) as CHEMBARAMBAKKAM_mean,
    COUNT(*) as record_count
FROM processed_data
GROUP BY Year
ORDER BY Year
"""

yearly_duckdb = conn.execute(yearly_sql).df()
print(yearly_duckdb.head(10))

# Análisis mensual
print("\n--- Análisis por mes ---")
monthly_sql = """
WITH processed_data AS (
    SELECT 
        EXTRACT(month FROM strptime(Date, '%d-%m-%Y')) as Month,
        MONTHNAME(strptime(Date, '%d-%m-%Y')) as Month_Name,
        (POONDI + CHOLAVARAM + REDHILLS + CHEMBARAMBAKKAM) as Total_Water,
        CASE 
            WHEN (POONDI + CHOLAVARAM + REDHILLS + CHEMBARAMBAKKAM) < 100 THEN 'Crítico'
            WHEN (POONDI + CHOLAVARAM + REDHILLS + CHEMBARAMBAKKAM) < 300 THEN 'Bajo'
            WHEN (POONDI + CHOLAVARAM + REDHILLS + CHEMBARAMBAKKAM) < 600 THEN 'Normal'
            ELSE 'Alto'
        END as Water_Level_Category
    FROM 'data/chennai_reservoir_levels.csv'
)
SELECT 
    Month,
    Month_Name,
    ROUND(AVG(Total_Water), 2) as Total_Water_mean,
    ROUND(MAX(Total_Water), 2) as Total_Water_max,
    ROUND(MIN(Total_Water), 2) as Total_Water_min,
    COUNT(*) as record_count
FROM processed_data
GROUP BY Month, Month_Name
ORDER BY Month
"""

monthly_duckdb = conn.execute(monthly_sql).df()
print(monthly_duckdb)

# Análisis por categoría
print("\n--- Análisis por categoría de nivel de agua ---")
category_sql = """
WITH processed_data AS (
    SELECT 
        strptime(Date, '%d-%m-%Y') as parsed_date,
        (POONDI + CHOLAVARAM + REDHILLS + CHEMBARAMBAKKAM) as Total_Water,
        EXTRACT(year FROM strptime(Date, '%d-%m-%Y')) as Year,
        CASE 
            WHEN (POONDI + CHOLAVARAM + REDHILLS + CHEMBARAMBAKKAM) < 100 THEN 'Crítico'
            WHEN (POONDI + CHOLAVARAM + REDHILLS + CHEMBARAMBAKKAM) < 300 THEN 'Bajo'
            WHEN (POONDI + CHOLAVARAM + REDHILLS + CHEMBARAMBAKKAM) < 600 THEN 'Normal'
            ELSE 'Alto'
        END as Water_Level_Category
    FROM 'data/chennai_reservoir_levels.csv'
)
SELECT 
    Water_Level_Category,
    COUNT(*) as Date_count,
    ROUND(AVG(Total_Water), 2) as Total_Water_mean,
    MIN(Year) as Year_min,
    MAX(Year) as Year_max
FROM processed_data
GROUP BY Water_Level_Category
ORDER BY 
    CASE Water_Level_Category 
        WHEN 'Crítico' THEN 1 
        WHEN 'Bajo' THEN 2 
        WHEN 'Normal' THEN 3 
        WHEN 'Alto' THEN 4 
    END
"""

category_duckdb = conn.execute(category_sql).df()
print(category_duckdb)

# Exportar resultados usando COPY TO
print("\n--- Exportando resultados ---")

# Exportar análisis anual
conn.execute("""
COPY (
    WITH processed_data AS (
        SELECT 
            EXTRACT(year FROM strptime(Date, '%d-%m-%Y')) as Year,
            (POONDI + CHOLAVARAM + REDHILLS + CHEMBARAMBAKKAM) as Total_Water,
            (POONDI + CHOLAVARAM + REDHILLS + CHEMBARAMBAKKAM) / 4.0 as Average_Level,
            POONDI, CHOLAVARAM, REDHILLS, CHEMBARAMBAKKAM
        FROM 'data/chennai_reservoir_levels.csv'
    )
    SELECT 
        Year,
        ROUND(AVG(Total_Water), 2) as Total_Water_mean,
        ROUND(MAX(Total_Water), 2) as Total_Water_max,
        ROUND(MIN(Total_Water), 2) as Total_Water_min,
        ROUND(STDDEV(Total_Water), 2) as Total_Water_std,
        ROUND(AVG(Average_Level), 2) as Average_Level_mean,
        COUNT(*) as record_count
    FROM processed_data
    GROUP BY Year
    ORDER BY Year
) TO 'outputs/duckdb_yearly_analysis.csv' (HEADER, DELIMITER ',')
""")
print("✓ Análisis anual exportado a: outputs/duckdb_yearly_analysis.csv")

# Exportar análisis mensual
conn.execute("""
COPY (
    WITH processed_data AS (
        SELECT 
            EXTRACT(month FROM strptime(Date, '%d-%m-%Y')) as Month,
            MONTHNAME(strptime(Date, '%d-%m-%Y')) as Month_Name,
            (POONDI + CHOLAVARAM + REDHILLS + CHEMBARAMBAKKAM) as Total_Water
        FROM 'data/chennai_reservoir_levels.csv'
    )
    SELECT 
        Month,
        Month_Name,
        ROUND(AVG(Total_Water), 2) as Total_Water_mean,
        ROUND(MAX(Total_Water), 2) as Total_Water_max,
        ROUND(MIN(Total_Water), 2) as Total_Water_min,
        COUNT(*) as record_count
    FROM processed_data
    GROUP BY Month, Month_Name
    ORDER BY Month
) TO 'outputs/duckdb_monthly_analysis.csv' (HEADER, DELIMITER ',')
""")
print("✓ Análisis mensual exportado a: outputs/duckdb_monthly_analysis.csv")

# También exportar dataset completo procesado a Parquet
conn.execute("""
COPY (
    SELECT 
        strptime(Date, '%d-%m-%Y') as parsed_date,
        Date,
        POONDI, CHOLAVARAM, REDHILLS, CHEMBARAMBAKKAM,
        (POONDI + CHOLAVARAM + REDHILLS + CHEMBARAMBAKKAM) as Total_Water,
        EXTRACT(year FROM strptime(Date, '%d-%m-%Y')) as Year,
        EXTRACT(month FROM strptime(Date, '%d-%m-%Y')) as Month,
        (POONDI + CHOLAVARAM + REDHILLS + CHEMBARAMBAKKAM) / 4.0 as Average_Level,
        CASE 
            WHEN (POONDI + CHOLAVARAM + REDHILLS + CHEMBARAMBAKKAM) < 100 THEN 'Crítico'
            WHEN (POONDI + CHOLAVARAM + REDHILLS + CHEMBARAMBAKKAM) < 300 THEN 'Bajo'
            WHEN (POONDI + CHOLAVARAM + REDHILLS + CHEMBARAMBAKKAM) < 600 THEN 'Normal'
            ELSE 'Alto'
        END as Water_Level_Category,
        CASE 
            WHEN POONDI >= CHOLAVARAM AND POONDI >= REDHILLS AND POONDI >= CHEMBARAMBAKKAM THEN 'POONDI'
            WHEN CHOLAVARAM >= POONDI AND CHOLAVARAM >= REDHILLS AND CHOLAVARAM >= CHEMBARAMBAKKAM THEN 'CHOLAVARAM'
            WHEN REDHILLS >= POONDI AND REDHILLS >= CHOLAVARAM AND REDHILLS >= CHEMBARAMBAKKAM THEN 'REDHILLS'
            ELSE 'CHEMBARAMBAKKAM'
        END as Dominant_Reservoir
    FROM 'data/chennai_reservoir_levels.csv'
    ORDER BY parsed_date
) TO 'outputs/duckdb_complete_dataset.parquet' (FORMAT PARQUET)
""")
print("✓ Dataset completo exportado a: outputs/duckdb_complete_dataset.parquet")

# Cerrar conexión
conn.close()

print(f"\n--- Resumen DuckDB ---")
print(f"✓ Consultas SQL ejecutadas exitosamente")
print(f"✓ Análisis replicado desde pandas")
print(f"✓ Resultados exportados en múltiples formatos")