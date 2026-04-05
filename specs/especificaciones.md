# Especificaciones del Proyecto: Benchmark TPC-DS — Lakehouse SQL Endpoint vs Fabric Warehouse

**Versión**: 1.1  
**Autor**: Nelson López  
**Fecha**: 2026-04-05  
**Estado**: Revisado

---

## 1. Objetivo

Comparar el rendimiento del **SQL endpoint de un Fabric Lakehouse** frente a un **Fabric Warehouse** ejecutando un conjunto representativo de queries inspiradas en el benchmark **TPC-DS**, bajo distintas condiciones de escala de datos, estado de caché y configuración de tablas.

El resultado del proyecto es un conjunto de métricas objetivas (latencias, fiabilidad) que permitan tomar decisiones fundadas sobre qué motor SQL usar en Microsoft Fabric según el patrón de carga de trabajo.

---

## 2. Alcance

### Dentro del alcance
- Ejecución y medición de queries SELECT sobre dos endpoints SQL de Fabric
- Variación de escala de datos: SF10 (~10 GB), SF100 (~100 GB), SF1000 (~1 TB)
- Medición con caché fría (cold) y caché caliente (warm)
- Varias configuraciones de tabla en el Lakehouse: sin partición, particionado por fecha, Z-order, V-order
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

El nombre del workspace es configurable mediante el argumento `--workspace` o la variable de entorno `FABRIC_WORKSPACE_NAME`.
El nombre del Lakehouse es configurable mediante el argumento `--lh` o la variable de entorno `FABRIC_LAKEHOUSE_NAME`.
El nombre del Warehouse es configurable mediante el argumento `--wh` o la variable de entorno `FABRIC_WAREHOUSE_NAME`.

El aprovisionamiento se realiza mediante el script `provision/setup_fabric.py`, que usa **Azure CLI** (`az rest`) con autenticación interactiva (`az login`).

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
| SF10 | ~10 GB | Pruebas iniciales, validación de queries |
| SF100 | ~100 GB | Pruebas de rendimiento medio |
| SF1000 | ~1 TB | Pruebas de rendimiento a escala real |

### Generación de datos
- Herramienta: **dsdgen** (TPC-DS Data Generator, parte de `tpcds-kit`)
- Formato de salida: **CSV** (formato nativo de dsdgen, sin conversión adicional)
- Script: `data_generation/generate_csv.py`
- Los datos se almacenan en `data/sfXX/` y **no se incluyen en el repositorio** (excluidos por `.gitignore`)

---

## 5. Configuraciones de tabla en el Lakehouse

Se probará el mismo conjunto de queries sobre tres configuraciones de las tablas Delta en el Lakehouse:

| Config | Descripción | Partición | Z-Order | V-Order |
|--------|-------------|-----------|---------|---------|
| `default` | Sin optimizaciones adicionales | Ninguna | No | No |
| `partitioned` | Particionado por columna de fecha | `ss_sold_date_sk` | No | No |
| `zorder` | Z-order en columnas de join/filtro frecuentes | Ninguna | `ss_item_sk`, `ss_store_sk` | No |
| `vorder` | V-Order habilitado en todas las tablas | Ninguna | No | **Sí** |

El Warehouse **no tiene configuraciones variables** — se prueba con la configuración estándar.

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
Endpoints:    lakehouse_default | lakehouse_partitioned | lakehouse_zorder | lakehouse_vorder | warehouse
Scale factor: SF10 | SF100 | SF1000
Queries:      Q1 | Q2 | Q3 | Q4 | Q5
Caché:        cold (1 rep) | warm (3 reps)
```

**Total de ejecuciones**:
- Cold: 5 endpoints × 3 SF × 5 queries × 1 rep = **75 ejecuciones**
- Warm: 5 endpoints × 3 SF × 5 queries × 3 reps = **225 ejecuciones**
- **Total: 300 ejecuciones** + 3 ciclos de pausa/reanudación de capacidad (uno por SF)

### Orden de ejecución por bloque de scale factor

Las ejecuciones se agrupan por bloque de SF para minimizar los ciclos de pausa/reanudación (3 en total):

```
Para cada scale_factor en [SF10, SF100, SF1000]:
  1. Reanudar capacidad → polling hasta estado Active
  2. Bloque cold: ejecutar todos los (endpoint × query) UNA vez
     → primera ejecución tras reanudación = cold real (cachés vacíos)
  3. Bloque warm: ejecutar todos los (endpoint × query) 3 veces
     → capacidad ya caliente, cachés precargados
  4. Pausar capacidad → polling hasta estado Paused

Al terminar el último SF: capacidad queda pausada (ya cubierto por el paso 4 del último bloque)
```

- **Cold** (primera ejecución tras reanudación de capacidad): garantiza cachés de memoria completamente vacíos
- **Warm** (ejecuciones sobre capacidad caliente): misma conexión, motor ya con datos en caché

> ⚠️ **Nota operativa**: las operaciones de pausa y reanudación pueden tardar entre 3 y 8 minutos. El módulo `provision/capacity_manager.py` implementa polling con reintentos hasta confirmar el estado final antes de continuar.

---

## 8. Métricas capturadas

Por cada ejecución individual se registra:

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `run_id` | UUID | Identificador único de la ejecución |
| `timestamp` | datetime | Momento de inicio de la ejecución |
| `endpoint` | string | `lakehouse_default`, `lakehouse_partitioned`, `lakehouse_zorder`, `lakehouse_vorder`, `warehouse` |
| `scale_factor` | string | `SF10`, `SF100`, `SF1000` |
| `query_id` | string | `q01`–`q05` |
| `cache_mode` | string | `cold`, `warm` |
| `repetition` | int | Número de repetición (1, 2, 3) |
| `elapsed_ms` | float | Tiempo transcurrido en milisegundos (medición client-side) |
| `rows_returned` | int | Número de filas devueltas |
| `status` | string | `success`, `error`, `timeout` |
| `error_message` | string | Mensaje de error (si aplica) |

**Salida**: `results/benchmark_{timestamp}.csv` y `results/benchmark_{timestamp}.json`

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
│   ├── 01_lakehouse_ingest.ipynb  # Spark: CSV → Delta (Lakehouse)
│   ├── 02_warehouse_ingest.ipynb  # Spark/SQL: CSV → Warehouse
│   └── table_configs.py           # Configuraciones de tabla
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
