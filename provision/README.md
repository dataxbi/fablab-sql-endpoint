# provision/

Scripts de aprovisionamiento inicial de los recursos Fabric necesarios para el benchmark.

## Ficheros

| Fichero | Rol |
|---------|-----|
| `setup_fabric.py` | Script de ejecución única: crea workspace, Lakehouse y Warehouse vía API |
| `capacity_manager.py` | Módulo reutilizable: pausa/reanuda la capacidad Fabric con polling |

---

## setup_fabric.py

Crea (o verifica que existan) los tres recursos Fabric necesarios e imprime los strings de conexión SQL para copiarlos al `.env`.

**Cuándo usarlo:** una sola vez al inicio del proyecto, o si se repite el experimento desde cero en un entorno nuevo.

```bash
py provision/setup_fabric.py [--workspace NAME] [--lh NAME] [--wh NAME] [--capacity-id ID]
```

### Argumentos

| Argumento | Variable de entorno | Default |
|-----------|---------------------|---------|
| `--workspace` | `FABRIC_WORKSPACE_NAME` | `FabLab_SQL_Endpoint` |
| `--lh` | `FABRIC_LAKEHOUSE_NAME` | `LH_01` |
| `--wh` | `FABRIC_WAREHOUSE_NAME` | `WH_01` |
| `--capacity-id` | `FABRIC_CAPACITY_ID` | *(opcional)* |

### Ejemplo de salida

```
============================================================
PROVISIONING COMPLETE
============================================================
Workspace     : FabLab_SQL_Endpoint  (id: f67c4250-...)

Lakehouse     : LH_01  (id: d10ce80e-...)
  Server      : evxidzw3ig3u...datawarehouse.fabric.microsoft.com
  Database    : d10ce80e-...

Warehouse     : WH_01  (id: ...)
  Server      : evxidzw3ig3u...datawarehouse.fabric.microsoft.com
  Database    : WH_01
============================================================
Copy the values above into your .env file.
```

> El script es idempotente: si los recursos ya existen, los detecta y no los duplica.

---

## capacity_manager.py

Módulo de librería — **no se ejecuta directamente**. Expone dos funciones que usa `benchmark/runner.py` durante la ejecución del benchmark:

| Función | Descripción |
|---------|-------------|
| `resume_capacity(...)` | Reanuda la capacidad y espera hasta estado `Active` |
| `pause_capacity(...)` | Pausa la capacidad y espera hasta estado `Paused` |
| `get_capacity_state(...)` | Consulta el estado actual de la capacidad |

Ambas funciones son bloqueantes: hacen polling cada 15 s hasta alcanzar el estado esperado o agotar el timeout (por defecto 10 minutos).

### Por qué es necesario

La única forma fiable de vaciar todas las cachés en memoria de Fabric (para mediciones *cold*) es pausar y reanudar la capacidad. El runner llama a estas funciones al inicio de cada bloque de escala (`SF10`, `SF100`).

---

## Prerrequisitos

```bash
az login   # autenticación vía Azure CLI (sin service principals)
```

Las credenciales se leen del entorno — ver `.env.example` en la raíz del repositorio.
