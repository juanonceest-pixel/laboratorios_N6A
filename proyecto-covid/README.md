Pipeline de Datos COVID-19
Proyecto de Ingeniería de Datos - Análisis Epidemiológico Ecuador vs Perú


📋 Descripción del Proyecto
Este proyecto implementa un pipeline automatizado de datos para el análisis de métricas epidemiológicas de COVID-19, comparando la evolución de la pandemia entre Ecuador y Perú. Utiliza datos oficiales de Our World in Data (OWID) y está construido con Dagster como orquestador de datos.
Objetivos

Automatizar la ingesta y procesamiento de datos COVID-19
Calcular métricas epidemiológicas estandarizadas para comparación internacional
Implementar validaciones de calidad de datos en tiempo real
Generar reportes ejecutivos para toma de decisiones

🏗️ Arquitectura del Proyecto
covid-pipeline/
│
├── 📁 covid_pipeline/              # Módulo principal Dagster
│   ├── __init__.py                # Definiciones de assets y checks
│   └── assets.py                  # Implementación de pipeline completo
│
├── 📁 docs/                       # Documentación del proyecto
│   └── INFORME_TECNICO.md         # Análisis técnico detallado
│
├──              
│   ├── tabla_perfilado.csv        # Estadísticas exploratorias
│   └── reporte_covid_resultados.xlsx # Reporte ejecutivo final
│
├── 📄 eda_exploracion.py          # Script de análisis exploratorio
├── 📄 requirements.txt            # Dependencias del proyecto
├── 📄 pyproject.toml             # Configuración Dagster
└── 📄 README.md                  # Este archivo
Componentes del Sistema
🔧 Pipeline de Datos (Assets)

leer_datos - Ingesta automática desde OWID
datos_procesados - Limpieza y filtrado de datos
metrica_incidencia_7d - Incidencia por 100k habitantes
metrica_factor_crec_7d - Factor de crecimiento semanal
reporte_excel_covid - Exportación de resultados

✅ Sistema de Validación (Asset Checks)

check_fechas_futuras - Validación temporal
check_columnas_clave - Integridad de datos
check_incidencia_rango - Validación de métricas

🚀 Instalación y Configuración
Prerrequisitos

Python 3.8 o superior
Git
Conexión a internet (para descarga de datos)

1. Clonar el Repositorio
bashgit clone <repository-url>
cd covid-pipeline
2. Crear Entorno Virtual
bash# Crear entorno virtual
python -m venv venv

# Activar entorno virtual
# En Windows:
venv\Scripts\activate

# En macOS/Linux:
source venv/bin/activate
3. Instalar Dependencias
bashpip install -r requirements.txt
4. Verificar Instalación
bashdagster --version
📊 Ejecución del Proyecto
Paso 1: Análisis Exploratorio de Datos (EDA)
bashpython eda_exploracion.py
Resultado esperado:

✅ Se descarga automáticamente el dataset de OWID
✅ Se genera outputs/tabla_perfilado.csv
✅ Se muestran estadísticas descriptivas en consola

Paso 2: Pipeline Automatizado con Dagster
bashdagster dev
Resultado esperado:

✅ UI de Dagster se abre en http://localhost:3000
✅ Todos los assets aparecen en la interfaz
✅ Asset checks están disponibles para validación

Paso 3: Ejecución del Pipeline

Navegar a http://localhost:3000
Ir a la sección "Assets"
Hacer clic en "Materialize all"
Monitorear ejecución en tiempo real

Paso 4: Verificar Resultados
Los siguientes archivos deben generarse automáticamente:

outputs/tabla_perfilado.csv - Estadísticas exploratorias
outputs/reporte_covid_resultados.xlsx - Reporte final con métricas

📈 Métricas Calculadas
A. Incidencia Acumulada a 7 días por 100k habitantes
Fórmula: (new_cases / population) * 100,000
Ventana: Promedio móvil de 7 días
Interpretación: Estandariza casos por población, facilita comparación entre países
B. Factor de Crecimiento Semanal
Fórmula: casos_semana_actual / casos_semana_anterior
Interpretación:

> 1.0 = Crecimiento de casos
< 1.0 = Decrecimiento de casos
= 1.0 = Estabilidad

🛡️ Sistema de Validación
Validaciones de Entrada

Fechas futuras: Verifica ausencia de datos temporalmente inconsistentes
Columnas críticas: Confirma presencia de campos esenciales
Integridad referencial: Validación de claves primarias

Validaciones de Salida

Rangos epidemiológicos: Métricas dentro de límites esperados
Consistencia temporal: Coherencia en series de tiempo
Calidad de resultados: Verificación de outputs finales

📋 Estructura de Datos
Dataset de Entrada (OWID)
Fuente: https://catalog.ourworldindata.org/garden/covid/latest/compact/compact.csv
Columnas principales:
- country: País
- date: Fecha
- new_cases: Casos nuevos diarios
- people_vaccinated: Personas vacunadas
- population: Población total
Reportes Generados
1. tabla_perfilado.csv
Estadísticas descriptivas del análisis exploratorio inicial.
2. reporte_covid_resultados.xlsx
Hoja 1 - Datos_Procesados: Dataset limpio y filtrado
Hoja 2 - Incidencia_7d: Métricas de incidencia por fecha y país
Hoja 3 - Factor_Crec_7d: Factores de crecimiento semanales
🔧 Solución de Problemas
Error: "Module not found"
bash# Verificar entorno virtual activo
pip list | grep dagster

# Reinstalar dependencias
pip install -r requirements.txt
Error: "Port already in use"
bash# Usar puerto alternativo
dagster dev -p 3001
Error: "No data found for countries"
bash# Verificar conexión a internet
ping catalog.ourworldindata.org

# El script detecta automáticamente países disponibles
Error en Asset Checks

Fechas futuras: Normal en datasets con proyecciones
Valores faltantes: Típico en datos de vacunación (comenzó en 2021)
Rangos atípicos: Revisar períodos de picos pandémicos

📊 Interpretación de Resultados
Análisis Comparativo Ecuador vs Perú
Factores a considerar:

Diferencias poblacionales: Ecuador ~17M, Perú ~33M habitantes
Capacidad de testing: Influye en detección de casos
Políticas sanitarias: Confinamientos, restricciones
Cobertura de vacunación: Cronograma y acceso

Métricas Clave para Análisis

Picos pandémicos: Identificar ondas epidemiológicas
Eficacia de intervenciones: Correlación políticas-casos
Velocidad de vacunación: Impacto en reducción de casos
Estacionalidad: Patrones temporales regionales

📚 Documentación Técnica
Documentos de Referencia

Informe Técnico - Análisis arquitectónico completo
Dagster Documentation - Documentación oficial
OWID COVID-19 Data - Fuente de datos

APIs y Tecnologías

Dagster: Orquestación de pipelines de datos
Pandas: Manipulación y análisis de datos
Requests: Descarga automática de datasets
OpenPyXL: Generación de reportes Excel