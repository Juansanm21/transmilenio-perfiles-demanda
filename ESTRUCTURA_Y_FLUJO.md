# Estructura del proyecto y flujo de trabajo

> Este archivo es para el equipo (no solo para Claude). Edítalo cuando algo
> cambie: una carpeta nueva, un paso que se salta, quién quedó a cargo de qué.
> Las decisiones "oficiales" del proyecto (rúbrica, cronograma, hipótesis)
> están en `CLAUDE.md` — este archivo es el mapa rápido y el reparto de tareas.

_Última actualización: 30 de septiembre de 2026._

---

## 1. Carpetas del repositorio

```
Proyecto Final/
│
├── README.md              # qué es el proyecto y cómo correrlo
├── CLAUDE.md               # decisiones, rúbrica, cronograma (uso interno/Claude)
├── ESTRUCTURA_Y_FLUJO.md   # este archivo
├── requirements.txt        # librerías de Python a instalar
├── .env.example            # plantilla de credenciales de AWS (sin datos reales)
├── .gitignore               # qué NO se sube a GitHub (datos crudos, credenciales)
├── docker-compose.yml       # para levantar Grafana en la máquina local
│
├── notebooks/               # el análisis, un notebook por fase
│   ├── 01_entendimiento_datos.ipynb
│   ├── 02_eda.ipynb
│   ├── 03_benchmark_dask.ipynb
│   └── 04_modelado_y_priorizacion.ipynb
│
├── src/                     # el mismo código, reutilizable (no duplicar en cada notebook)
│   ├── features.py             # construye las variables por estación
│   ├── db.py                   # conexión a la base de datos en AWS
│   ├── flows/pipeline_transmilenio.py  # flujo de Prefect (ETL automático)
│   └── tasks/                  # cada paso del flujo, por separado
│
├── sql/schema.sql           # tablas de la base de datos de resultados en AWS
├── grafana/                 # dashboard.json exportado
├── docs/                    # capturas y diagramas de evidencia para el documento
│
└── data/                    # LOS DATOS. Existen en cada computador, NO se suben a GitHub
    ├── raw/validaciones_troncal/   # ZIPs diarios descargados de TransMilenio
    └── raw/estaciones/             # lat/lon de cada estación
```

**Importante:** `data/` no está en GitHub (ver `.gitignore`). Cada integrante
debe descargar los mismos archivos en su propio computador para que el
código corra igual. El detalle de qué días descargar está en `CLAUDE.md`.

---

## 2. Flujo de trabajo (de principio a fin)

| # | Paso | Dónde vive | Estado |
|---|------|-----------|--------|
| 1 | Descargar ZIPs diarios de TransMilenio + estaciones | `data/raw/` | ✅ Hecho |
| 2 | Inventario: columnas reales, tamaño, tipos de dato | `notebooks/01_entendimiento_datos.ipynb` | ✅ Hecho |
| 3 | Limpieza, cruce con estaciones, EDA y variables por estación | `notebooks/02_eda.ipynb` | ✅ Hecho |
| 4 | Repetir el procesamiento con Dask, 2+ configuraciones, medir tiempo/memoria | `notebooks/03_benchmark_dask.ipynb` | ⏳ Siguiente |
| 5 | Modelo: perfiles de demanda + prioridad de refuerzo + anomalías | `notebooks/04_modelado_y_priorizacion.ipynb` | ⏳ Pendiente |
| 6 | Empaquetar los pasos 3-5 en un flujo que se repita solo con datos nuevos | `src/flows/pipeline_transmilenio.py` | ⏳ Pendiente |
| 7 | Subir solo los resultados (nunca el dato crudo) a una base de datos en AWS | `sql/schema.sql`, `src/db.py` | ⏳ Pendiente |
| 8 | Dashboard gerencial conectado a esa base | `grafana/` | ⏳ Pendiente |
| 9 | Documento final con evidencia de cada paso | Google Docs (por crear) | ⏳ Pendiente |

---

## 3. Reparto de tareas (ajustar con nombres reales)

| Persona | Se encarga principalmente de |
|---|---|
| Juan Pablo | Notebooks 01-02 (datos, limpieza, features), coordinación general |
| Persona 2 | Prefect + Dask + AWS (ingeniería, notebook 03, `src/flows`, `src/db.py`) |
| Persona 3 | Grafana + documento + presentación |

> Todos deben poder explicar cualquier parte del proyecto — la idea de
> repartir es solo para tener un responsable claro por tarea, no encasillar
> a nadie.

---

## 4. Cómo correr el proyecto en tu computador

```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env      # y pedirle a Juan Pablo las credenciales reales de AWS
```

Cada integrante crea su propia carpeta `venv/` dentro del proyecto (no se sube a GitHub).

Los notebooks se corren en orden (01 a 04). Si algo cambia en la forma de
descargar los datos o en las carpetas, actualizar este archivo.
