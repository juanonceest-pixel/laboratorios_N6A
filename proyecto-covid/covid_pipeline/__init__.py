from dagster import Definitions
from .assets import (
    leer_datos,
    datos_procesados,
    metrica_incidencia_7d,
    metrica_factor_crec_7d,
    reporte_excel_covid,
    check_fechas_futuras,
    check_columnas_clave,
    check_incidencia_rango
)

defs = Definitions(
    assets=[
        leer_datos,
        datos_procesados,
        metrica_incidencia_7d,
        metrica_factor_crec_7d,
        reporte_excel_covid
    ],
    asset_checks=[
        check_fechas_futuras,
        check_columnas_clave,
        check_incidencia_rango
    ]
)