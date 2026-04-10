# Especificaciones del Proyecto: Benchmark TPC-DS — Lakehouse SQL Endpoint vs Fabric Warehouse

**Versión**: 1.8  
**Autor**: Nelson López  
**Fecha**: 2026-04-10  
**Estado**: Revisado

---

## 1. Objetivo

Comparar el rendimiento del **SQL endpoint de un Fabric Lakehouse** frente a un **Fabric Warehouse** ejecutando un conjunto representativo de queries inspiradas en el benchmark **TPC-DS**, bajo distintas condiciones de escala de datos, estado de caché y configuración de tablas.

El resultado del proyecto será un conjunto de métricas objetivas (latencias, fiabilidad) que permitirán tomar decisiones fundadas sobre qué motor SQL usar en Microsoft Fabric según el patrón de carga de trabajo.

---

## 2. Alcance

### Dentro del alcance
- Ejecución y medición de queries SELECT sobre dos endpoints SQL de Fabric
- Variación de escala de datos: SF10 (~10 GB), SF100 (~100 GB)
- Medición con caché fría (cold) y caché caliente (warm)
- Varias configuraciones de tabla en el Lakehouse: sin partición, particionado por fecha, V-order
- Generación de datos TPC-DS en formato CSV (herramienta externa, no se mide)
- Ingesta de datos en Lakehouse y Warehouse (no se mide)
- Exportación de resultados a CSV y JSON
- Análisis comparativo mediante notebook Python en Fabric

### Fuera del alcance
- Medición del tiempo de generación de datos o ingesta
- Optimización de capacidad Fabric (SKU, escalado automático)
- Queries de escritura (INSERT, UPDATE, DELETE)
- Comparación con otros motores externos a Fabric (Databricks, Synapse, etc.)
- Configuración de Row-Level Security u otras políticas de seguridad

---

## 3. Entornos de Fabric

| Recurso | Nombre | Tipo |
|---------|--------|------|
| Workspace | `FabLab_SQL_Endpoint` (configurable) | Fabric Workspace |
| Lakehouse | `LH_01` (con esquema) (configurable) | Fabric Lakehouse |
| Warehouse | `WH_01` (configurable) | Fabric Warehouse |

El nombre del workspace será configurable mediante el argumento `--workspace` o la variable de entorno `FABRIC_WORKSPACE_NAME`.
El nombre del Lakehouse será configurable mediante el argumento `--lh` o la variable de entorno `FABRIC_LAKEHOUSE_NAME`.
El nombre del Warehouse será configurable mediante el argumento `--wh` o la variable de entorno `FABRIC_WAREHOUSE_NAME`.

El aprovisionamiento se realizará mediante el script `provision/setup_fabric.py`, que usará **Azure CLI** (`az rest`) con autenticación interactiva (`az login`).

---

## 4. Dataset: TPC-DS

### Tablas principales utilizadas

| Tabla | Tipo | Descripción |
|-------|------|-------------|
| `store_sales` | Fact | Ventas en tienda física (tabla principal) |
| `date_dim` | Dimensión | Dimensión de tiempo/fecha |
| `item` | Dimensión | Catálogo de productos |
| `store` | Dimensión | Tiendas |
| `customer` | Dimensión | Clientes |
| `customer_demographics` | Dimensión | Demografía del cliente |
| `promotion` | Dimensión | Promociones |
| `household_demographics` | Dimensión | Demografía del hogar |

### Factores de escala (SF)

| Factor | Tamaño aproximado | Propósito |
|-------------|-------------------|-----------|
| SF10 | ~10 GB | Validación de la ingesta y del runner (no se incluye en los resultados finales) |
| SF100 | ~100 GB | Benchmark de rendimiento — única escala publicada en los resultados |

> **Nota**: SF10 se utilizó para probar el pipeline de ingesta (dsdgen → CSV → Delta → OPTIMIZE) y verificar que el runner ejecuta correctamente las queries. Los resultados de SF10 no forman parte del informe final de rendimiento.

### Generación de datos
- Herramienta: **dsdgen** (TPC-DS Data Generator, parte de `tpcds-kit`)
- Formato de salida: **CSV** (formato nativo de dsdgen, sin conversión adicional)
- Script: `data_generation/generate_csv.py`
- Los datos se almacenarán en `data/sfXX/` y **no se incluirán en el repositorio** (excluidos por `.gitignore`)

---

## 5. Configuraciones de tabla en el Lakehouse

Se probará el mismo conjunto de queries sobre tres configuraciones de las tablas Delta en el Lakehouse:

| Config | Descripción | Partición | V-Order |
|--------|-------------|-----------|---------|
| `default` | Sin optimizaciones adicionales | Ninguna | No |
| `partitioned` | Particionado por columna de fecha | `ss_sold_date_sk` | No |
| `vorder` | V-Order habilitado en todas las tablas | Ninguna | **Sí** |

> **Nota sobre OPTIMIZE**: tras la ingesta de cada configuración se ejecuta `OPTIMIZE` (sin ZORDER) en todas las tablas para compactar ficheros Parquet pequeños generados por Spark. Esto no constituye una configuración de benchmark en sí misma, sino una práctica estándar de mantenimiento Delta. El ZORDER fue descartado por su coste desproporcionado a partir de SF100 (decenas de horas a SF1000 incluso en F128).

> **Nota sobre schemas de Lakehouse**: cada configuración se escribe en un schema independiente dentro del mismo Lakehouse: `benchmark_default`, `benchmark_partitioned` y `benchmark_vorder`. Las queries SQL no incluyen prefijo de schema; el runner establece el schema de trabajo con `USE {schema}` inmediatamente después de conectar, antes de ejecutar cada query.

> **Nota sobre schemas de ingesta (StructType)**: los notebooks/scripts de ingesta utilizan **schemas explícitos** (`StructType`) para cada tabla TPC-DS — nunca `inferSchema`. Esto garantiza tipos consistentes (e.g. `LongType` para claves SK, `DecimalType(7,2)` para importes) independientemente del scale factor. La columna de partición en `benchmark_partitioned` es `ss_sold_date_sk` (nombre real de columna según el schema explícito). Las definiciones completas están en `ingestion/table_configs.py`.

 > **Nota sobre ingesta en el Warehouse**: las tablas del Warehouse se poblan mediante **CTAS cross-database** (`CREATE TABLE benchmark.<tabla> AS SELECT * FROM [LH_01].[benchmark_default].<tabla>`), ejecutado vía `sqlcmd` directamente contra el SQL endpoint de WH_01. Este enfoque aprovecha la capacidad de consulta cross-database de Fabric (mismo workspace) y es más sencillo y fiable que el conector Spark `com.microsoft.fabric.spark.write`, que solo funciona en notebooks nativos de Fabric y no está disponible en sesiones Livy externas. El script T-SQL está en `ingestion/02_warehouse_ingest.sql`.

El Warehouse **no tendrá configuraciones variables** — se probará con la configuración estándar.

---

## 6. Queries de benchmark

Cinco queries SQL representativas, basadas en TPC-DS y adaptadas para ser comparables entre Lakehouse y Warehouse:

### Q1 — Agregación simple
- **Inspiración TPC-DS**: Q29
- **Descripción**: Suma de ventas y conteo de transacciones agrupados por tienda y mes
- **Operaciones**: `SUM`, `COUNT`, `GROUP BY`, `ORDER BY`
- **Tablas**: `store_sales`, `date_dim`, `store`
- **Archivo**: `sql/q01_simple_agg.sql`

### Q2 — Join grande (star schema)
- **Inspiración TPC-DS**: Q19
- **Descripción**: Ventas totales por producto, marca y clase de tienda, con join a cuatro dimensiones
- **Operaciones**: `JOIN` × 4, `GROUP BY`, `ORDER BY`, `LIMIT`
- **Tablas**: `store_sales`, `date_dim`, `item`, `store`, `customer`
- **Archivo**: `sql/q02_large_join.sql`

### Q3 — Top N con filtros selectivos
- **Inspiración TPC-DS**: Q6 / Q42
- **Descripción**: Top 10 artículos por ingresos en una categoría y período específicos
- **Operaciones**: `WHERE` (filtros selectivos en fecha y categoría), `ORDER BY`, `LIMIT`
- **Tablas**: `store_sales`, `date_dim`, `item`
- **Archivo**: `sql/q03_top_n_selective.sql`

### Q4 — Query compleja tipo TPC-DS real
- **Inspiración TPC-DS**: Q72 / Q14
- **Descripción**: Análisis de inventario con CTEs, subqueries correlacionadas y múltiples joins
- **Operaciones**: CTEs (`WITH`), subqueries, `JOIN` × 5+, predicados complejos
- **Tablas**: `store_sales`, `date_dim`, `item`, `promotion`, `household_demographics`, `customer_demographics`
- **Archivo**: `sql/q04_complex_tpcds.sql`

### Q5 — Función ventana analítica
- **Inspiración TPC-DS**: Q35 / Q86
- **Descripción**: Ranking de clientes por gasto total usando funciones de ventana
- **Operaciones**: `RANK()`, `ROW_NUMBER()`, `PARTITION BY`, `ORDER BY`
- **Tablas**: `store_sales`, `customer`, `date_dim`
- **Archivo**: `sql/q05_window_function.sql`

---

## 7. Matriz de pruebas

```
Endpoints:    lakehouse_default | lakehouse_partitioned | lakehouse_vorder | warehouse
Scale factor: SF100 (único)
Queries:      Q1 | Q2 | Q3 | Q4 | Q5
Caché:        cold (1 rep) | warm (3 reps)
```

> **Nota**: SF10 se usó exclusivamente para validar la ingesta. El benchmark final corre solo con SF100.

**Total de ejecuciones**:
- Cold: 4 endpoints × 1 SF × 5 queries × 1 rep = **20 ejecuciones**
- Warm: 4 endpoints × 1 SF × 5 queries × 3 reps = **60 ejecuciones**
- **Total: 80 ejecuciones** + 1 ciclo de pausa/reanudación de capacidad

### Orden de ejecución

```
1. Reanudar capacidad → polling hasta estado Active
2. Bloque cold: ejecutar todos los (endpoint × query) UNA vez
   → primera ejecución tras reanudación = cold real (cachés vacíos)
3. Bloque warm: ejecutar todos los (endpoint × query) 3 veces
   → capacidad ya caliente, cachés precargados
4. Pausar capacidad → polling hasta estado Paused
```

---

## 8. Métricas capturadas

Por cada ejecución individual se registrará:

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `run_id` | UUID | Identificador único de la ejecución |
| `timestamp` | datetime | Momento de inicio de la ejecución |
| `endpoint` | string | `lakehouse_default`, `lakehouse_partitioned`, `lakehouse_vorder`, `warehouse` |
| `scale_factor` | string | `SF100` (único factor de escala en el benchmark final) |
| `query_id` | string | `q01`–`q05` |
| `cache_mode` | string | `cold`, `warm` |
| `repetition` | int | Número de repetición (1, 2, 3) |
| `elapsed_ms` | float | Tiempo transcurrido en milisegundos (medición client-side) |
| `rows_returned` | int | Número de filas devueltas |
| `status` | string | `success`, `error`, `timeout` |
| `error_message` | string | Mensaje de error (si aplica) |

**Salida**: los resultados se guardarán en `results/benchmark_{timestamp}.csv` y `results/benchmark_{timestamp}.json`

---

## 9. Estructura del proyecto

```
fablab-sql-endpoint/
├── .github/
│   └── copilot-instructions.md    # Instrucciones para GitHub Copilot (inglés)
├── specs/
│   └── especificaciones.md        # Este documento
├── provision/
│   ├── setup_fabric.py            # Aprovisionamiento Fabric vía Azure CLI
│   └── capacity_manager.py        # Pausa/reanudación de capacidad Fabric (usado en cold cache)
├── data_generation/
│   ├── generate_csv.py            # dsdgen wrapper → CSV
│   └── README.md
├── ingestion/
│   ├── 01_lakehouse_ingest.ipynb  # Spark: CSV → Delta (Lakehouse, 3 configs)
│   ├── 02_warehouse_ingest.sql    # T-SQL CTAS cross-DB: LH_01 → WH_01
│   └── table_configs.py           # Configuraciones de tabla y schemas StructType
├── sql/
│   ├── q01_simple_agg.sql
│   ├── q02_large_join.sql
│   ├── q03_top_n_selective.sql
│   ├── q04_complex_tpcds.sql
│   └── q05_window_function.sql
├── benchmark/
│   ├── runner.py                  # Ejecutor principal
│   ├── config.yaml                # Matriz de pruebas
│   ├── connection.py              # Gestión de conexiones pyodbc
│   └── utils.py                  # Timer, logging, serialización
├── results/                       # Salida CSV/JSON (excluida de git)
├── analysis/
│   └── analyze_results.ipynb      # Análisis comparativo
├── .env.example                   # Plantilla de variables de entorno
├── .gitignore
└── requirements.txt
```

---

## 10. Dependencias técnicas

| Dependencia | Uso |
|-------------|-----|
| `pyodbc` | Conexión a SQL endpoints de Fabric |
| `pandas` | Procesamiento de resultados y análisis en notebooks |
| `pyyaml` | Lectura de `config.yaml` |
| `python-dotenv` | Carga de variables de entorno desde `.env` |
| `azure-identity` | Autenticación con Fabric REST API |
| `requests` | Llamadas a Fabric REST API (aprovisionamiento y gestión de capacidad) |
| `matplotlib` / `seaborn` | Visualización en el notebook de análisis |
| dsdgen (externo) | Generación de datos TPC-DS en formato CSV |
| Azure CLI (`az`) | Autenticación y aprovisionamiento Fabric |

---

## 11. Convención de autoría en Git

| Tipo de archivo | Autor del commit | Co-autor |
|-----------------|-----------------|----------|
| `specs/`, `.github/copilot-instructions.md` | Nelson López `<nelson.lopez@dataxbi.com>` | `Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>` |
| Todo el código (`provision/`, `benchmark/`, `sql/`, `data_generation/`, `ingestion/`, `analysis/`) | `GitHub Copilot <223556219+Copilot@users.noreply.github.com>` | — |

### Protocolo de actualización de documentación
Cada vez que se apruebe una modificación del plan:
1. Actualizar este documento (`specs/especificaciones.md`)
2. Actualizar `.github/copilot-instructions.md`
3. Hacer commit de la documentación actualizada **antes** del commit de código

---

## 12. Variables de entorno

Todas las configuraciones sensibles se gestionan mediante variables de entorno (nunca hardcodeadas en el código). Ver `.env.example` para la plantilla completa.

| Variable | Descripción | Valor por defecto |
|----------|-------------|-------------------|
| `FABRIC_WORKSPACE_NAME` | Nombre del workspace en Fabric | `FabLab_SQL_Endpoint` |
| `FABRIC_LAKEHOUSE_NAME` | Nombre del Lakehouse en Fabric | `LH_01` |
| `FABRIC_WAREHOUSE_NAME` | Nombre del Warehouse en Fabric | `WH_01` |
| `FABRIC_CAPACITY_ID` | ID de la capacidad Fabric F/P SKU | — |
| `LAKEHOUSE_SERVER` | FQDN del SQL endpoint del Lakehouse | — |
| `LAKEHOUSE_DATABASE` | Nombre de la base de datos del Lakehouse | — |
| `WAREHOUSE_SERVER` | FQDN del SQL endpoint del Warehouse | — |
| `WAREHOUSE_DATABASE` | Nombre de la base de datos del Warehouse | — |
| `TENANT_ID` | ID del tenant de Azure AD | — |
