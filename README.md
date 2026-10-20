# Proyecto Big Data: TransMilenio (troncal)

Proyecto del curso **Herramientas de Big Data** (Maestría en Analítica). Versión 2, construida paso a paso en equipo.

> La versión anterior (v1) quedó como respaldo: `Proyecto Final - respaldo v1 (2026-10-05).zip` y la carpeta `Proyecto Final`.

Pregunta del proyecto: ¿Qué estaciones de la troncal de TransMilenio requieren refuerzo de personal de apoyo para ordenar el ingreso de usuarios, según su demanda por franja horaria?

## Estado

EDA terminado. Flujo de Prefect, carga a AWS y tablero de Grafana funcionando. Análisis de personal y modelo simple en preparación.

## Notebooks

| Notebook | Contenido |
|---|---|
| `01_inventario_y_carga_dask` | Inventario de los 21 archivos, conversión a Parquet y comparación pandas / Dask con varias configuraciones |
| `02_exploracion_y_limpieza` | Columnas, vacíos, calidad y decisiones de limpieza |
| `03_eda_demanda_y_patrones` | **El EDA**: días, franjas, estaciones, accesos, perfiles, ida y vuelta, correlaciones e hipótesis |
| `04_ranking_y_personal_de_apoyo` | Responde la pregunta: umbral y ventana de refuerzo, ranking por franja, estaciones piloto y personal de apoyo |
| `05_modelo_simple` | Modelo simple: lee la tabla de AWS, validación temporal, líneas base y un modelo sencillo |
| `anexo_teorema_limite_central` | Verificación del Teorema del Límite Central con la hora de la validación |

## Flujo, base de datos y tablero

```
ZIP diarios → exploración con Dask (notebooks 01 a 03: fija las reglas y los parámetros) → [Prefect + Dask: aplica las reglas] → tabla analisis_estacion_hora (AWS RDS PostgreSQL) → Grafana
```

- `src/pipeline_transmilenio.py`: flujo de Prefect. Cuando hay una **semana completa** (7 días) que aún no está en la base, la procesa con Dask, calcula el ranking y carga el resultado con *upsert* (si se repite, no duplica). Si no hay semana nueva, termina sin hacer nada.
- Tabla única en AWS: `analisis_estacion_hora`, una fila por semana × estación × hora, con el análisis incluido (ranking, ventana de refuerzo, personas en el pico). **Solo resultados agregados**: nunca validaciones individuales ni tarjetas.
- `grafana/dashboard.json` y `docker-compose.yml`: tablero conectado a AWS con un usuario de solo lectura.

## Estructura

```
├── notebooks/            el análisis, un notebook por fase
├── src/                  flujo de Prefect (pipeline_transmilenio.py)
├── grafana/              tablero (dashboard.json) y su configuración
├── docker-compose.yml    Grafana local
├── data/                 NO se sube a GitHub: raw/ (originales) y processed/ (resultados y caché)
└── docs/                 figuras (figs/) y tablas de decisiones
```

## Cómo ejecutar

```bash
python -m venv venv
source venv/Scripts/activate        # Git Bash en Windows
pip install -r requirements.txt
cp .env.example .env                # llenar con los datos de la base (nunca se suben)
```

Flujo de Prefect (con el entorno activo):

```bash
python src/pipeline_transmilenio.py                          # una corrida
python src/pipeline_transmilenio.py --fecha-corte 2026-08-09 # simula que solo llegaron los archivos hasta esa fecha
python src/pipeline_transmilenio.py --workers 2 --memoria 4GB # otra configuración de Dask
python src/pipeline_transmilenio.py --servir                 # lo deja programado (todos los días a las 6:00)
```

Tablero de Grafana:

```bash
docker compose up -d     # http://localhost:3000
```

## Reglas del proyecto

- Nunca se sube a AWS ni a GitHub el dataset original: solo resultados.
- Credenciales solo en `.env` (nunca en el código).
- Cada notebook explica qué hace cada celda y deja evidencia para el documento.



