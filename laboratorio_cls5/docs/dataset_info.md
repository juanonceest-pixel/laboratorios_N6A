# Dataset: Crime Data from 2020 to Present

## Información General
- **Fuente**: Los Angeles Police Department
- **Período**: 2020 - Presente  
- **Formato**: CSV
- **Delimitador**: Coma (,)
- **Codificación**: UTF-8

## Descripción de Columnas

| Columna | Descripción | Tipo | Validación |
|---------|-------------|------|------------|
| DR_NO | Número de reporte único | String | Debe ser único |
| Date Rptd | Fecha de reporte | String | Formato MM/DD/YYYY |
| DATE OCC | Fecha de ocurrencia | String | Formato MM/DD/YYYY |
| TIME OCC | Hora de ocurrencia | String | Formato HHMM |
| AREA | Código de área | Integer | Debe ser > 0 |
| AREA NAME | Nombre del área | String | No puede estar vacío |
| Crm Cd | Código de crimen | Integer | Debe ser > 0 |
| Crm Cd Desc | Descripción del crimen | String | No puede estar vacío |
| Vict Age | Edad de la víctima | Integer | Rango 0-120 |
| Vict Sex | Sexo de la víctima | String | M, F, X o vacío |
| Status | Estado del caso | String | IC, CC, AO, JO |
| LAT | Latitud | Float | Rango 33-35 (LA area) |
| LON | Longitud | Float | Rango -119 a -117 (LA area) |

## Reglas de Validación Implementadas

1. **Columnas Obligatorias**: Verificar presencia de columnas críticas
2. **DR_NO Único**: No debe haber duplicados
3. **Coordenadas Válidas**: LAT/LON dentro del rango de Los Angeles
4. **Edades Válidas**: Entre 0 y 120 años
5. **Valores Categóricos**: Sexo debe ser M, F, X o vacío