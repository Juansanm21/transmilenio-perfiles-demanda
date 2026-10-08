# Proyecto Big Data: TransMilenio (troncal)

Proyecto del curso **Herramientas de Big Data** (Maestría en Analítica). Versión 2, construida paso a paso en equipo.

> La versión anterior (v1) quedó como respaldo: `Proyecto Final - respaldo v1 (2026-10-05).zip` y la carpeta `Proyecto Final`.

## Estado

EDA terminado y modelo en preparación.

| Notebook | Contenido |
|---|---|
| `01_inventario_y_carga_dask` | Inventario de los 21 archivos, conversión a Parquet y comparación pandas / Dask |
| `02_exploracion_y_limpieza` | Columnas, vacíos, calidad y decisiones de limpieza |
| `03_eda_demanda_y_patrones` | EDA ampliado: días, franjas, estaciones, accesos, perfiles, ida y vuelta, correlaciones e hipótesis |
| `04_teorema_limite_central` | Verificación del Teorema del Límite Central con la hora de la validación |
| `05_eda_prioridad_estaciones` | EDA final: ranking por franja, espacio y accesos, estaciones piloto y plan de apoyo |
| `06_modelo_demanda` | Modelo de predicción de validaciones por estación y hora: datos, validación temporal y líneas base (los modelos de aprendizaje de máquina están pendientes) |

Pregunta del proyecto: ¿qué estaciones de la troncal, y en qué franjas horarias de los días hábiles, concentran la mayor demanda de ingreso y justifican una mayor asignación de personal de apoyo para la gestión de filas y el orden?

## Estructura

```
├── notebooks/    el análisis, un notebook por fase (01, 02, ...)
├── src/          funciones reutilizables y el flujo de Prefect
├── data/         NO se sube a GitHub: raw/ (originales) y processed/ (resultados)
└── docs/         figuras (figs/) y capturas de evidencia
```

Otras carpetas (`sql/` para el esquema de AWS, `grafana/` para el tablero) se agregan cuando se llegue a ese paso.

## Cómo ejecutar

```bash
python -m venv venv
source venv/Scripts/activate        # Git Bash en Windows
pip install -r requirements.txt
cp .env.example .env                # credenciales (nunca se suben)
```

## Reglas del proyecto

- Nunca se sube a AWS ni a GitHub el dataset original: solo resultados.
- Credenciales solo en `.env` (nunca en el código).
- Cada notebook explica qué hace cada celda y deja evidencia para el documento.
