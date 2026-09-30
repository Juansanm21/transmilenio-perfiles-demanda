# Perfiles de demanda en la troncal de TransMilenio

Proyecto del curso **Herramientas de Big Data** (Maestría en Analítica). Identifica perfiles de demanda de las estaciones troncales de TransMilenio a partir de sus validaciones, para apoyar la priorización operativa en horas pico.

> El detalle completo de decisiones, rúbrica y cronograma está en [`CLAUDE.md`](./CLAUDE.md).

## Pregunta de estudio

¿Qué perfiles de demanda presentan las estaciones del componente troncal de TransMilenio según sus patrones temporales de validaciones, y qué estaciones requieren prioridad en la gestión operativa durante las horas pico?

## Datos

| Fuente | Contenido | Uso |
|---|---|---|
| [Validaciones diarias TMSA](https://storage.googleapis.com/validaciones_tmsa/validaciones_diarias.html) | Validaciones troncales por día (1 ZIP/CSV por día) | Insumo principal |
| [Estaciones troncales (datos abiertos TM)](https://datosabiertos-transmilenio.hub.arcgis.com/datasets/estaciones-troncales-de-transmilenio/) | Nombre, troncal, latitud/longitud de cada estación | Cruce geoespacial |

**Ventana de datos usada** (`data/raw/`, no se sube a GitHub — ver `.gitignore`):
- 10–16 de agosto de 2026 y 14–20 de septiembre de 2026: semanas típicas, sin festivo. Base del modelo de perfiles.
- 3–9 de agosto de 2026: incluye el festivo del 7 de agosto (Batalla de Boyacá). Caso de comparación hábil/fin de semana/festivo.

Total: ~2 GB comprimido / ~9.6 GB en CSV — supera el mínimo de 1.5 GB pedido por la rúbrica.

## Estructura del repositorio

```
├── notebooks/          # exploración y modelado (ver numeración por fase)
├── src/
│   ├── flows/           # flujo(s) de Prefect
│   ├── tasks/           # tareas individuales del flujo
│   ├── features.py      # construcción de variables por estación
│   └── db.py            # conexión y carga a AWS RDS
├── sql/schema.sql       # esquema de la base de datos de resultados
├── grafana/             # dashboard.json exportado
├── docs/                # diagramas y evidencias para el documento
└── data/                # vacío en el repo; datos crudos y procesados en local
```

## Cómo ejecutar

```bash
python -m venv venv
venv\Scripts\activate       # Windows
pip install -r requirements.txt
cp .env.example .env        # y completar credenciales de AWS RDS
```

La carpeta `venv/` queda dentro del proyecto (cada quien crea la suya, no se sube a GitHub — ver `.gitignore`).

Los notebooks se ejecutan en orden (`01_...` a `05_...`). El flujo de Prefect está en `src/flows/pipeline_transmilenio.py`.

## Equipo

3 integrantes — roles y reparto de tareas en `CLAUDE.md` (sección 6).

## Estado

Ver checklist en `CLAUDE.md` (sección "Estado actual").
