# Informe Técnico: Pipeline de Datos COVID-19

## 1. Arquitectura del Pipeline

### Descripción de Assets Creados

El pipeline implementa una arquitectura de assets encadenados en Dagster, siguiendo el flujo de procesamiento de datos desde la ingesta hasta la exportación final:

```
leer_datos → datos_procesados → [metrica_incidencia_7d, metrica_factor_crec_7d] → reporte_excel_covid
     ↓              ↓                           ↓
check_fechas_futuras   check_columnas_clave   check_incidencia_rango
```

#### Assets Implementados:

1. **leer_datos**: Asset de ingesta que descarga los datos COVID-19 desde la URL canónica de OWID
2. **datos_procesados**: Asset de transformación que limpia y filtra los datos para Ecuador y Perú
3. **metrica_incidencia_7d**: Asset que calcula la incidencia acumulada a 7 días por 100k habitantes
4. **metrica_factor_crec_7d**: Asset que calcula el factor de crecimiento semanal de casos
5. **reporte_excel_covid**: Asset de exportación que genera el archivo Excel final

#### Asset Checks Implementados:

1. **check_fechas_futuras**: Valida que no existan fechas futuras en los datos
2. **check_columnas_clave**: Verifica valores nulos en columnas esenciales (location, date, population)
3. **check_incidencia_rango**: Valida que la métrica de incidencia esté en el rango 0-2000

### Justificación de Decisiones de Diseño

- **Modularidad**: Cada paso del proceso se implementa como un asset independiente, permitiendo ejecución parcial y debugging granular
- **Trazabilidad**: Los Asset Checks proporcionan visibilidad inmediata de la calidad de datos en la UI de Dagster
- **Reproducibilidad**: Al usar la URL canónica, garantizamos datos actualizados y consistentes
- **Simplicidad**: Se optó por pandas para las transformaciones debido a la familiaridad y el tamaño manejable de los datos

## 2. Decisiones de Validación

### Chequeos de Entrada

**Reglas Aplicadas:**

1. **Validación de fechas futuras**: Garantiza integridad temporal de los datos
   - *Motivación*: Prevenir análisis con datos incorrectos o inconsistentes temporalmente
   
2. **Validación de columnas clave**: Verifica que location, date y population no tengan valores nulos
   - *Motivación*: Estas columnas son fundamentales para todos los cálculos posteriores

### Chequeos de Salida

**Reglas Aplicadas:**

1. **Validación de rango de incidencia**: Verifica que incidencia_7d esté entre 0 y 2000
   - *Motivación*: Valores fuera de este rango serían epidemiológicamente improbables y podrían indicar errores de cálculo

### Descubrimientos Importantes

Durante el análisis de los datos se identificaron:

- **Valores negativos** en new_cases debido a revisiones retrospectivas de casos
- **Períodos sin datos** en algunos países, particularmente en fases tempranas de la pandemia
- **Diferencias significativas** en la cobertura de datos de vacunación entre Ecuador y Perú

## 3. Consideraciones de Arquitectura

### Elección de Tecnologías

**Pandas vs. DuckDB vs. Soda:**

- **Pandas**: Seleccionado para transformaciones y cálculos de métricas por:
  - Facilidad de uso para operaciones de ventana móvil
  - Integración natural con Dagster
  - Tamaño de datos manejable (no requiere optimización para big data)

- **Dagster Asset Checks**: Elegidos sobre Soda para validaciones por:
  - Integración nativa con la UI de Dagster
  - Simplicidad de implementación
  - Visibilidad inmediata de resultados

### Patrones de Diseño Aplicados

- **Dependency Injection**: Assets dependen explícitamente de sus inputs
- **Separation of Concerns**: Cada asset tiene una responsabilidad única y bien definida
- **Fail Fast**: Validaciones tempranas previenen propagación de errores

## 4. Resultados

### Métricas Implementadas

#### A. Incidencia Acumulada a 7 días por 100k habitantes
- **Fórmula**: `(new_cases / population) * 100000`, promedio móvil de 7 días
- **Interpretación**: Estandariza por población y captura tendencia reciente de casos
- **Utilidad**: Comparación directa entre países con diferentes tamaños de población

#### B. Factor de Crecimiento Semanal
- **Fórmula**: `casos_semana_actual / casos_semana_previa`
- **Interpretación**: >1 indica crecimiento, <1 indica decrecimiento
- **Utilidad**: Identificación temprana de tendencias epidemiológicas

### Resumen del Control de Calidad

| Regla de Validación | Estado Esperado | Acción en Caso de Fallo |
|---------------------|-----------------|-------------------------|
| Fechas futuras | PASS | Documentar y continuar con warning |
| Columnas clave nulas | PASS | Investigar fuente de datos |
| Rango incidencia 0-2000 | PASS | Revisar cálculos de métricas |

### Archivos Generados

1. **tabla_perfilado.csv**: Resumen estadístico de la exploración inicial
2. **reporte_covid_resultados.xlsx**: Resultados finales con tres hojas:
   - Datos_Procesados: Dataset limpio y filtrado
   - Incidencia_7d: Métrica de incidencia por país y fecha
   - Factor_Crec_7d: Factor de crecimiento semanal

## 5. Conclusiones

El pipeline implementado cumple con los objetivos de:
- **Automatización completa** del procesamiento de datos COVID-19
- **Calidad de datos** mediante validaciones en entrada y salida
- **Reproducibilidad** a través de assets bien definidos
- **Observabilidad** mediante la UI de Dagster y Asset Checks

La arquitectura permite fácil extensión para incluir nuevos países, métricas adicionales o fuentes de datos complementarias.