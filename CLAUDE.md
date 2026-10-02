# CLAUDE.md — Proyecto Big Data: Perfiles de demanda en la troncal de TransMilenio

> Archivo de contexto para trabajar con Claude en este proyecto. Resume todas las decisiones tomadas hasta ahora, la rúbrica y el plan de trabajo. Claude debe leerlo al inicio de cada sesión y respetar las decisiones y convenciones descritas aquí.

---

## 1. Contexto del proyecto

- Proyecto grupal (3 integrantes) del curso de **Big Data** de la maestría.
- Responsable de esta sesión: **Juan Pablo Sánchez** (ingeniero mecánico, business analyst; maneja Python, SQL y Power BI).
- **Presentación:** 10 de octubre de 2026. Un integrante elegido al azar presenta un resumen ejecutivo de 10 minutos. Para esa fecha todo el análisis debe estar listo para responder cualquier punto de la rúbrica.
- **Entrega final del documento:** una semana después (17 de octubre de 2026), incorporando la retroalimentación.
- Idioma de trabajo: **español** (código con nombres de variables y comentarios en español cuando sea natural).

---

## 2. Rúbrica (requisitos obligatorios)

Conjunto de datos **superior a 1.5 GB**, trabajado con metodología **CRISP-DM**:

| # | Requisito |
|---|---|
| a | Identificar el conjunto de datos, definir el objetivo de análisis y la unidad de análisis |
| b | EDA: descriptiva, tipos de datos, tendencia central, distribuciones, patrones; construir variable objetivo y características |
| c | Análisis preliminar e identificar si se requiere información adicional de contexto |
| d | Al menos un flujo con **Prefect** recurrente y escalable (si llegan más datos, el ETL los transforma y rehace la analítica) |
| e | Usar **Dask** evidenciando al menos **dos configuraciones** de workers y memoria por worker |
| f | Base de datos en **Amazon AWS** con los resultados. **SOLO se sube el resultado de la analítica, NUNCA el dataset original** |
| g | Visualización en **Grafana** para análisis a nivel gerencial |
| h | Conclusiones del proceso |
| i | Documento completo con normas **APA o IEEE** (se eligió IEEE) |
| j | Documento en Google Docs / Word colaborativo, compartido con **rodolfo.meza@gmail.com** con permiso de edición |
| k | El documento debe incluir resultados, explicación de fases y evidencias de todo el proceso |
| l | Notebooks y scripts de Python en un repositorio de **GitHub** |

---

## 3. Decisiones tomadas

### Dataset
- **Elegido:** validaciones del **componente troncal de TransMilenio** (portal de datos abiertos de TransMilenio).
- **Descartados:** US Accidents (el profesor lo considera cliché) y datasets de Oil & Gas (no todo el grupo conoce el dominio).
- **Alternativas de respaldo** evaluadas por si TransMilenio falla (acceso o calidad de datos): eCommerce Behavior Data REES46 (Kaggle, la mejor alternativa), H&M Personalized Fashion (Kaggle), Backblaze Hard Drive Stats.

**Ventana de datos descargada (2026-09-30), en `data/raw/` (no se sube a GitHub):**
- Formato real: un ZIP por día en `storage.googleapis.com/validaciones_tmsa/ValidacionTroncal/`, ~120-125 MiB comprimido entre semana, ~30-80 MiB fin de semana/festivo. Un día entre semana descomprime a ~609 MB (ratio de expansión ~4.8x).
- **10-16 agosto 2026** y **14-20 septiembre 2026**: dos semanas típicas, sin festivo, a propósito en meses distintos (para que el perfil "normal" no dependa de una sola racha de clima o evento puntual de dos semanas seguidas). Es la base del modelo de clustering.
- **3-9 agosto 2026**: incluye el festivo del 7 de agosto (Batalla de Boyacá). Se usa solo como caso de comparación puntual para la pregunta de hábil/fin de semana/festivo, no entra al set de entrenamiento del modelo principal.
- Total: ~2 GB comprimido, ~9.6 GB en CSV estimado (evidencia de que supera 1.5 GB).
- Estaciones (lat/lon): `data/raw/estaciones/`. Dos archivos por ahora (API de ArcGIS Hub `estaciones_troncales.csv`, 150 estaciones, y descarga desde la UI `estaciones_troncales_ui.csv`, 153 registros, columnas distintas) — unificar y elegir uno como canónico en el notebook 01.
- **Columnas reales confirmadas** (Validación Troncal, ver notebook 01 para el detalle completo): `Acceso_Estacion, Day_Group_Type, Dispositivo, Emisor, Estacion_Parada, Fase, Fecha_Clearing, Fecha_Transaccion, Hora_Pico_SN, ID_Vehiculo, Linea, Nombre_Perfil, Numero_Tarjeta, Operador, Ruta, Saldo_Previo_a_Transaccion, Saldo_Despues_Transaccion, Sistema, Tipo_Tarifa, Tipo_Tarjeta, Tipo_Vehiculo, Valor`. No trae lat/lon (de ahí el cruce con estaciones).
- **Corrección importante sobre columnas que parecían útiles y no lo son:** `Day_Group_Type` y `Hora_Pico_SN` vienen **constantes** (siempre "Dia 1" y "Peak Time" respectivamente) en cada archivo diario — NO distinguen hábil/festivo ni hora pico como se asumía antes. Hay que derivar ambas cosas nosotros mismos a partir de `Fecha_Transaccion` (día de la semana + calendario propio de festivos; hora pico AM 5-9h / PM 16-20h). `ID_Vehiculo`, `Ruta`, `Tipo_Vehiculo` vienen 100% nulos (solo aplican a Zonal/Dual, se descartan). `Sistema` es constante "TRONCAL" (sin uso analítico). Los CSV deben leerse con `encoding='utf-8'` (ver corrección de encoding más abajo — antes se documentó mal como latin1). Hay un pequeño "sangrado" de transacciones (~0.01-0.02%) entre archivos de días consecutivos cerca de medianoche — no son duplicados (verificado), pero al agregar hay que agrupar por la fecha real de `Fecha_Transaccion`, no por el archivo de origen. La clave para cruzar con el dataset de estaciones es el código de 5 dígitos al inicio de `Estacion_Parada` (ej. `"(05002) Tintal"`), no el nombre completo.
- **Corrección de encoding (encontrada el 1-oct al revisar una figura):** el archivo es **UTF-8**, no latin1. Se verificó con los bytes crudos: `\xc3\xa9` es exactamente la codificación UTF-8 de "é" (confirmado con `ord()` en Python, sin depender de cómo lo muestra una terminal — eso fue justo lo que generó la confusión inicial). Usar `encoding='latin1'` corrompe cada tilde en 2 caracteres (ej. "América" → "AmÃ©rica"). Ya corregido en notebooks 01 y 02 y reconstruidos `fact_demanda_estacion_hora.parquet` y `estacion_features.parquet`. Si algo (CSV, figura) muestra nombres con `Ã` y no se ha regenerado después del 1-oct, es este bug — volver a ejecutar con `encoding='utf-8'`.

### Pregunta de estudio

**Reformulada el 1-oct, a partir de la revisión de un integrante del equipo** (ver razones abajo). La pregunta anterior mezclaba dos objetivos (perfiles + prioridades) y usaba lenguaje de "control de acceso/evasión" que no se puede sostener con los datos (no hay forma de cuantificar colados con validaciones). La nueva pregunta deja un solo objetivo principal (prioridad de refuerzo por estación-día-hora) y usa los perfiles como medio, no como fin, con un argumento que sí se sostiene con los datos: medimos cuánta gente entra, dónde y cuándo, y eso justifica personal para **gestión de filas y aglomeraciones**, no control de evasión.

> **¿En qué estaciones troncales de TransMilenio y en qué franjas horarias de la semana se concentra la presión de demanda que justifica asignar personal de apoyo para la gestión de filas y el orden, y qué perfiles de demanda permiten organizar esos turnos?**

Preguntas específicas:
1. ¿Cómo se distribuye la demanda entre estaciones, troncales y franjas horarias?
2. ¿Qué diferencias existen entre la demanda de días hábiles y la de fines de semana o festivos?
3. ¿Qué grupos de estaciones con comportamiento similar se identifican a partir de sus curvas de demanda? (medio, no fin: sirve para organizar los turnos por tipo de estación, ej. "las estaciones del perfil residencial necesitan refuerzo de 5 a 8 a.m.; las de destino laboral, de 4 a 7 p.m.")
4. ¿Qué estaciones y horarios muestran concentraciones de demanda o comportamientos atípicos que ameriten intervención operativa?
5. **¿En qué estaciones y en qué franjas horarias de la semana se concentra la presión de demanda que justifica asignar personal de apoyo para gestión de filas y orden (no control de evasión — eso no se puede medir con estos datos)?**

> Motivación de la pregunta 5: en Bogotá hay personal (formal o informal, propio o de terceros) dedicado a esto en algunas estaciones, pero no está siempre ni en todas. El índice de prioridad operativa (Fase 4) se calcula a nivel estación-hora precisamente para responder esto: convertirlo en una recomendación de turnos.

- **Usuario gerencial:** Secretaría de Movilidad / gerencia de operaciones de TransMilenio — en particular, quien decide la contratación y los turnos de personal de apoyo en estaciones.
- **Objetivo general:** cuantificar la presión de demanda por estación-día-hora para priorizar dónde y cuándo asignar personal de apoyo (gestión de filas y orden), usando los perfiles de demanda (clustering) como herramienta para organizar esos turnos por tipo de estación.
- **Unidad de análisis:** la combinación **estación × día × hora** (no solo la estación) — es la unidad que se prioriza y sobre la que se decide un turno.
- **Variable objetivo principal:** **índice de prioridad de refuerzo** de cada combinación estación-día-hora (combina volumen de validaciones, concentración en el pico, y atipicidad). El perfil/cluster de demanda es una variable de apoyo (para agrupar estaciones con necesidades de turno similares), no la variable objetivo.
- **Hipótesis:** las estaciones se agrupan en pocos perfiles (residenciales, destino laboral, transferencia/portales, baja demanda) y la presión en hora pico se concentra en un subconjunto pequeño de combinaciones estación-hora.
- **Criterio de éxito:** un ranking estación-día-hora defendible con los datos (sin inferir evasión) y clusters interpretables que permitan agrupar recomendaciones de turno por tipo de estación.

**Nota de lenguaje (aplica a todo el proyecto, documento y presentación):** nunca usar "control de acceso", "evasión" o "colados" como justificación — usar siempre "gestión de filas", "aglomeraciones" u "orden en el flujo de pasajeros". Ya corregido en el notebook 02 (1-oct).

---

## 4. Stack tecnológico

Python · Pandas · **Dask** · **Prefect** · scikit-learn · Pandera · Parquet · **PostgreSQL en AWS RDS** (capa gratuita) · **Grafana** (Cloud gratuito o Docker) · GitHub · Google Docs.

---

## 5. Plan por fases CRISP-DM

### Fase 1 — Entendimiento del negocio (punto a)
Contexto de saturación en hora pico, usuario, objetivos, unidad de análisis, hipótesis y criterio de éxito (ver sección 3).

### Fase 2 — Entendimiento de los datos (puntos b y c)
1. **Inventario:** tamaño total (con captura que evidencie > 1.5 GB), archivos, filas, columnas, tipos, diccionario de datos. Verificar qué columnas trae realmente el archivo (¿identificador anonimizado de tarjeta?, ¿tipo de tarifa?, ¿acceso/línea?).
2. **Calidad:** nulos, duplicados, fechas fuera de rango, nombres de estación inconsistentes, registros en horas sin operación. Tabla con % de registros afectados y decisión tomada.
3. **EDA descriptivo:**
   - Tendencia central y dispersión de validaciones por estación, hora y día.
   - Histogramas (se espera distribución muy sesgada).
   - Curva horaria general y por troncal (picos AM y PM).
   - Día hábil vs. fin de semana vs. festivo.
   - Heatmap estación × hora.
   - **Mapa de calor geoespacial** de la demanda sobre la ciudad (lat/lon de cada estación, intensidad = validaciones), no solo un mapa de puntos por cluster. Es una de las razones por las que se eligió este dataset (poder cruzar con coordenadas). Herramienta sugerida en el notebook: `folium` o `plotly` (density_mapbox); en Grafana: panel Geomap con capa de calor además del de puntos por cluster.
   - Top 15 estaciones por demanda total y por demanda en hora pico.
   - Curva de Lorenz / índice de Gini de la demanda entre estaciones.
   - Si existe tipo de tarifa: participación de subsidios, adulto mayor y discapacidad.
4. **Features por estación:**
   - Demanda total y promedio diario.
   - % de demanda en pico AM (5–9 h) y pico PM (16–20 h).
   - Razón pico AM / pico PM (clave para distinguir residencial vs. destino).
   - Razón fin de semana / día hábil.
   - Curva horaria normalizada (24 valores que suman 1).
   - Índice de concentración en pico (hora máxima / promedio horario).
5. **Contexto adicional (punto c):**
   - Necesario: coordenadas de estaciones (GTFS o datos abiertos) y calendario de festivos.
   - Deseable: lluvia (IDEAM), capacidad/frecuencia de buses por troncal, eventos de esas semanas (marchas, cierres).
   - Documentar qué se consiguió y qué no. Sin capacidad solo se mide **presión de demanda**, no saturación real.

### Fase 3 — Preparación de datos
- Estandarizar nombres de estaciones → tabla maestra `dim_estacion` (id, nombre, troncal, lat, lon).
- Convertir CSV → **Parquet particionado por fecha** (documentar reducción de tamaño y tiempo de lectura).
- Agregar a nivel **estación–fecha–hora** (tabla central del proyecto).
- Validación de esquema con **Pandera**.

### Dask (punto e)
Usar Dask en lectura, limpieza y agregación de los datos crudos. Comparar con `LocalCluster`:

| Config | Workers | Hilos/worker | Memoria/worker |
|---|---|---|---|
| C1 | 2 | 2 | 4 GB |
| C2 | 4 | 1 | 2 GB |
| C3 (extra) | 1 | 4 | 8 GB |
| Línea base (extra) | Pandas sin Dask | — | — |

Medir tiempo y uso de memoria de cada una. Guardar capturas del **dashboard de Dask** (task stream, memoria por worker). Entregar tabla + gráfica comparativa y conclusión sobre paralelismo vs. memoria por worker.

### Fase 4 — Modelado
1. **Clustering de estaciones (principal):** StandardScaler → K-Means con k = 2…8; elegir k con codo + **silhouette**; validar con Davies-Bouldin; comparar con clustering jerárquico (dendrograma) y opcionalmente DBSCAN; visualizar con PCA 2D. Interpretar cada cluster con su curva horaria promedio y darle nombre de negocio.
2. **Detección de anomalías (secundario):** Isolation Forest o desviación frente al patrón típico de cada estación por hora y tipo de día. Cruzar con eventos reales.
3. **Índice de prioridad operativa (extra):** puntaje 0–100 por **estación-franja horaria** (no solo por estación) combinando demanda en pico, concentración y frecuencia de anomalías. Responde la pregunta 5: dónde y cuándo se necesita personal de control/orden. Salida pensada como recomendación de turnos por estación (ej. tabla estación × franja horaria × día de la semana → "requiere refuerzo sí/no"), no solo un ranking estático.
4. **Pronóstico (opcional/trabajo futuro):** LightGBM por estación–hora, solo como prueba de concepto (dos semanas es poco).
5. **Si hay ID de tarjeta (opcional):** segmentación de usuarios y aproximación origen-destino.

### Flujo con Prefect (punto d)
Flow `pipeline_transmilenio` con tareas:
1. `detectar_archivos_nuevos`
2. `validar_esquema` (Pandera)
3. `limpiar_y_convertir` (→ Parquet)
4. `agregar_con_dask`
5. `construir_features`
6. `asignar_clusters` (carga modelo entrenado)
7. `detectar_anomalias`
8. `calcular_kpis`
9. `cargar_a_rds` (upsert)
10. `registrar_ejecucion` (tabla `etl_log`)

Requisitos: parámetros (carpeta, rango de fechas, `reentrenar=True/False`), **idempotencia** (upsert por fecha + estación), `retries=2` en carga, **deployment con programación diaria**.
**Evidencia:** procesar semana 1, luego "llega" semana 2 y el flow la procesa sola. Capturas de la UI de Prefect.

### Base de datos en AWS (punto f)
RDS PostgreSQL capa gratuita (db.t3.micro / db.t4g.micro). **Solo resultados.**

| Tabla | Contenido |
|---|---|
| `dim_estacion` | id, nombre, troncal, coordenadas |
| `fact_demanda_estacion_hora` | validaciones agregadas por estación, fecha y hora |
| `estacion_perfil` | cluster, features, índice de prioridad |
| `cluster_resumen` | tamaño y características promedio por cluster |
| `anomalias` | estación, fecha, hora, observado vs. esperado |
| `kpi_diario` | totales diarios por troncal |
| `etl_log` | historial de ejecuciones del flujo |

Seguridad: usuario de solo lectura para Grafana; credenciales en `.env` (nunca en GitHub); security group restringido a IPs del equipo; alerta de facturación; eliminar la instancia al terminar. Incluir diagrama entidad-relación en el documento.

**Nota — cuenta AWS Academy:** el equipo usa una cuenta de AWS Academy (Learner Lab), no una cuenta AWS normal. Implicaciones a validar apenas se entre a la consola: la sesión y las credenciales suelen expirar (usualmente ~4 h) y tocar reiniciar el "Lab" cada vez que se retoma el trabajo; puede no haber IAM completo para crear usuarios nuevos; y la instancia de RDS podría borrarse si el Lab se resetea entre sesiones. Documentar apenas se confirme, y tener un plan B (ej. exportar dump de la BD al final de cada sesión de trabajo) si el Lab no persiste entre sesiones.

### Grafana (punto g)
Conectado a RDS. Paneles:
1. KPIs: validaciones totales, promedio diario, estación top, hora pico.
2. **Geomap** de estaciones coloreadas por cluster, tamaño por demanda, **con capa de mapa de calor** de intensidad de demanda (no solo puntos) — filtrable por franja horaria.
3. Curvas horarias promedio por cluster (panel clave).
4. Top 10 estaciones por índice de prioridad.
5. Heatmap estación × hora.
6. Hábil vs. fin de semana por troncal.
7. Tabla de anomalías.
8. Extras: filtros (troncal, cluster, fecha), panel de monitoreo del ETL (`etl_log`), una alerta configurada.

Exportar el JSON del dashboard al repo.

### Fase 5 — Evaluación y conclusiones (punto h)
- Responder cada pregunta específica con un dato concreto.
- Evaluar clusters (métricas, estabilidad al cambiar k, sentido de negocio).
- Recomendaciones gerenciales (ej.: refuerzo de flota en estaciones del cluster X de 5 a 7 a. m.; personal de gestión de filas en el top 10 de prioridad).
- Limitaciones: solo dos semanas, sin capacidad ni salidas, sin estacionalidad.
- Trabajo futuro: más meses, pronóstico, matriz origen-destino, integración SITP.

### Fase 6 — Documentación y despliegue (puntos i–l)

**Estructura del repositorio:**
```
proyecto-transmilenio/
├── README.md              # descripción, arquitectura, cómo ejecutar
├── requirements.txt
├── .env.example           # plantilla de credenciales (sin valores reales)
├── .gitignore             # incluye data/ y .env
├── docker-compose.yml     # Grafana + Prefect local (extra)
├── data/                  # vacía en el repo
├── notebooks/
│   ├── 01_entendimiento_datos.ipynb
│   ├── 02_eda.ipynb
│   ├── 03_benchmark_dask.ipynb
│   ├── 04_clustering.ipynb
│   └── 05_anomalias.ipynb
├── src/
│   ├── flows/pipeline_transmilenio.py
│   ├── tasks/
│   ├── features.py
│   └── db.py
├── sql/schema.sql
├── grafana/dashboard.json
└── docs/                  # diagramas y capturas de evidencia
```
Commits de los tres integrantes.

**Identidad de git en este repo:** configurada localmente (no global) como `Juan Pablo Sánchez <juansanm21@gmail.com>`, porque el repositorio/usuario de GitHub es **Juansanm21**, distinto del correo de trabajo que usa git por defecto en esta máquina (`jpsanchez@skanhawk.com`). No usar el correo de trabajo en los commits de este proyecto.

**Documento (Google Docs, normas IEEE):**
1. Portada · 2. Resumen/Abstract · 3. Introducción y contexto · 4. Pregunta, objetivos e hipótesis · 5. Marco de referencia · 6. Metodología y arquitectura · 7. Una sección por fase CRISP-DM con resultados y evidencias · 8. Dask (configuraciones y benchmark) · 9. Prefect (diseño y evidencia) · 10. AWS (modelo de datos y evidencia) · 11. Grafana (dashboard y lectura gerencial) · 12. Conclusiones, recomendaciones, limitaciones y trabajo futuro · 13. Referencias IEEE · 14. Anexos (enlaces a repo y dashboard).

Compartir con **rodolfo.meza@gmail.com** (edición) es un requisito de la rúbrica (punto j), pero el usuario pidió explícitamente **no compartir nada con el profesor todavía**. Crear el documento y trabajar en él con normalidad; la invitación a rodolfo.meza@gmail.com solo se envía cuando el usuario lo pida de forma explícita.

---

## 6. Cronograma

| Fechas | Fase | Entregable |
|---|---|---|
| 29–30 sep | Configuración y entendimiento de datos | Repo, datos descargados, diccionario de datos |
| 1–3 oct | Preparación y EDA | Parquet limpio, notebook de EDA |
| 3–5 oct | Dask y modelado | Benchmark Dask, clustering, anomalías |
| 5–6 oct | Prefect | Flow de punta a punta |
| 6–7 oct | AWS y Grafana | RDS con resultados, dashboard |
| 8 oct | Conclusiones, documento, diapositivas | Borrador completo |
| 9 oct | Ensayo (los tres presentan) | Presentación lista |
| **10 oct** | **Presentación** | — |
| 11–17 oct | Ajustes por retroalimentación | Documento final |

**Roles:** A = datos y modelado (EDA, features, clustering, anomalías) · B = ingeniería (Prefect, Dask, AWS, repo) · C = visualización y comunicación (Grafana, documento, presentación). Todos deben dominar todo.

---

## 7. Presentación (10 min)

| Tiempo | Contenido |
|---|---|
| 1 min | Problema y pregunta de estudio |
| 1 min | Datos y arquitectura (un diagrama) |
| 2 min | Hallazgos clave del EDA |
| 2 min | Clusters: mapa y curvas por perfil |
| 1.5 min | Prefect, Dask y AWS (evidencia breve) |
| 1.5 min | Dashboard de Grafana |
| 1 min | Conclusiones y recomendaciones |

Preparar diapositivas de respaldo técnicas (benchmark Dask completo, métricas de clusters, esquema BD).

---

## 8. Riesgos

| Riesgo | Mitigación |
|---|---|
| Descarga lenta o datos incompletos | Descargar de inmediato; tener REES46 como respaldo |
| Poca memoria en los equipos | Parquet + Dask desde el inicio; Google Colab si hace falta |
| Clusters poco interpretables | Probar solo curva horaria normalizada vs. features resumidas |
| Costos de AWS | Capa gratuita, alerta de facturación, borrar recursos al final |
| Sin coordenadas de estaciones | Geocodificar manualmente (~140 estaciones) |

---

## 9. Reglas para Claude en esta sesión

- Respetar las decisiones de la sección 3; no proponer cambiar de dataset salvo que el usuario lo pida o los datos resulten inviables.
- **Nunca** subir ni diseñar cargas del dataset original a AWS: solo tablas de resultados.
- Nunca escribir credenciales reales en código ni en archivos versionados; usar `.env` y `.env.example`.
- **Nunca compartir el Google Doc (ni ningún otro archivo del proyecto) con rodolfo.meza@gmail.com u otro correo del profesor sin que el usuario lo pida explícitamente en el momento.**
- **Los commits de este repositorio NO deben llevar línea de coautoría de IA** (nada de "Co-Authored-By: Claude..." ni similar). El uso de IA en el proceso es visible y está bien, pero el historial de commits no debe leerse como si la IA hubiera hecho todo el trabajo.
- Los tres integrantes tienen poco manejo técnico. Dividir el trabajo en tareas pequeñas y muy guiadas (una persona por notebook/sección, ver encabezados "Responsable sugerido" en cada notebook), y que los commits queden repartidos entre los tres para que se note el aporte de cada uno.
- Priorizar código reproducible: scripts en `src/`, exploración en `notebooks/`, rutas relativas y parámetros configurables.
- **El EDA (notebook 02) es la parte más importante del proyecto para el usuario, más que el modelo.** El clustering es el último paso; lo que de verdad se evalúa es qué tan bien justificado queda el conjunto de variables candidatas. Priorizar profundidad y rigor ahí (incluir `.head()` de los dataframes clave para que el equipo vea de qué se está hablando) antes de avanzar a Dask/clustering/Prefect/AWS.
- **Nunca justificar nada con "control de acceso" ni "evasión"/"colados"** — no hay datos para sostenerlo. Siempre hablar de "gestión de filas", "aglomeraciones" u "orden en el flujo de pasajeros" (ver sección 3, nota de lenguaje del 1-oct). El perfil/cluster de demanda es una herramienta para organizar turnos, no el objetivo principal — el objetivo principal es la prioridad de refuerzo por estación-día-hora.
- Cada paso importante debe dejar **evidencia** (capturas, tablas, logs) para el documento, porque la rúbrica la exige.
- Al terminar una tarea, indicar qué punto de la rúbrica (a–l) queda cubierto.
- Responder en español.

---

## 10. Estado actual

- [x] Dataset y pregunta de estudio definidos
- [x] Plan del proyecto definido
- [x] Datos descargados: 3 semanas de Troncal (~2 GB zip / ~9.6 GB CSV) + estaciones con lat/lon
- [x] Columnas reales confirmadas (ver sección 3)
- [x] Esqueleto del repositorio creado (`git init` local): notebooks, src/, sql/, .gitignore, README, requirements.txt
- [x] Repositorio remoto creado y con push: https://github.com/Juansanm21/transmilenio-perfiles-demanda (privado)
- [x] Entorno virtual en `venv/` dentro del proyecto (no en `C:\envs`, para que quede junto al código)
- [x] Notebook 01 con inventario real, diccionario y hallazgos de calidad
- [x] Notebook 02 con limpieza, cruce geoespacial, EDA y tabla de features por estación — ver hallazgos clave abajo
- [ ] Crear documento Google Docs (sin compartir con el profesor todavía, ver sección 9)
- [ ] Cuenta AWS Academy: validar duración de sesión/Lab y si persiste RDS entre sesiones
- [ ] Notebook 03 (Dask), 04 (clustering), 05 (anomalías), Prefect, AWS, Grafana

### Hallazgos clave del EDA (notebook 02), para citar en el documento final

- **31.5 millones de validaciones reales procesadas**, agregadas a 60,119 filas estación-fecha-hora, sin pérdida (la suma coincide exacta antes y después de la limpieza).
- **90.9% de las estaciones cruzan por código con el dataset geoespacial** (140/154); el resto son paraderos temporales por obras, corrales de bicicletas y 2 estaciones nuevas sin geolocalizar — documentado, no descartado a ciegas.
- **Corrección metodológica importante:** las semanas típicas son **exactamente 14 fechas explícitas** (10-16 ago + 14-20 sep: 10 hábiles, 2 sábados, 2 domingos). Antes se colaban como "días" parciales falsos las colas de medianoche del 17-ago y 13-sep (sangrado documentado en notebook 01), inflando a 17 días y contaminando los promedios por tipo de día. Ya corregido en todo el notebook.
- **El festivo se analiza en una sección totalmente aparte (2.10), sin mezclarse con el resto** — ni con el modelo, ni con el EDA general. El 7 de agosto tuvo 525,834 validaciones, casi igual al domingo de esa semana (516,447) y 3.5x menos que un hábil (1,856,994). Confirma H3, pero documentado como dato puntual (1 solo festivo), no generalizable a todos los festivos.
- **Concentración de demanda: Gini = 0.484**; las 15 estaciones de mayor demanda concentran el 37.8% del total (dominadas por portales/cabeceras de troncal). Apoya H1.
- **Arquetipos visibles incluso antes del clustering formal:** `razon_am_pm` alta (Tibanica-Primavera, Terreros, Tintal) = perfil residencial; baja (San Victorino, Museo del Oro, Virrey, Museo Nacional) = perfil de destino/comercio-cultura.
- **Rigor estadístico (mismo nivel que el análisis de series de tiempo del diplomado, Bogotá-Toronto):**
  - Boxplot por hora + verificación cruzada contra el propio tipo de día: de 26 candidatos, solo 2 (ambos del mismo evento: miércoles 16 sep., 22-23h, hasta 3.7x lo normal) son atípicos reales con base estadística sólida (10 hábiles para comparar); el resto son artefactos de mezclar tipos de día o de comparar contra un solo par (sábado/domingo, n=2).
  - Dickey-Fuller: serie horaria cruda NO estacionaria (p=0.221); residuo tras descomponer SÍ (p≈0).
  - ACF/PACF: eco claro cada ~24 rezagos (ciclo diario), igual que el eco cada 12 rezagos (ciclo anual) del análisis Bogotá-Toronto.
- **Mapa de calor geoespacial** general (`docs/mapa_calor_demanda.html`) y **por franja horaria** (`docs/figs/mapa_calor_por_franja_horaria.png`): el centro de gravedad de la demanda migra durante el día — portales en la mañana, zona comercial (Calle 100, Portal El Dorado) en el pico PM, y de noche cambia por completo a El Campín (estadio) y Héroes — ya no es commuting, es vida nocturna/eventos.
- **Por qué el pico AM parece más grande que el PM (duda real del usuario, resuelta con datos):** con la ventana estricta 16-20h, la mañana tiene 28.6% más validaciones que la tarde. Pero es un artefacto de la ventana: la salida matutina está muy concentrada (hora fija de llegada), el regreso se reparte en una ventana más ancha. Ampliando la tarde a 15-23h, la diferencia cae a solo 5.7%.
- **Matriz de correlación de las features candidatas:** `pct_pico_am` y `pct_pico_pm` están correlacionadas -0.92 (redundantes entre sí y con `razon_am_pm`). Se descartan las dos primeras del modelo; quedan `demanda_total`, `razon_am_pm`, `razon_finde_habil`, `indice_concentracion_pico` y la curva horaria normalizada completa, justificadas una por una en la sección 2.12 del notebook ("Variables candidatas para el modelo").
- **Recalibración de prioridades (pedido explícito del usuario):** el clustering es "lo último" — lo que de verdad importa es llegar a un EDA fuerte que proponga y justifique las variables del modelo, no apurar el modelo en sí. El notebook 02 ya cierra con una sección de conclusiones del EDA + variables candidatas, antes de tocar Dask o el clustering.
- **Por qué el mapa general (folium) muestra un punto caliente que nunca aparece como el más intenso en el mapa por franja horaria (duda real del usuario, resuelta):** `folium.HeatMap` difumina y SUMA círculos superpuestos; varias estaciones cercanas con demanda media-alta (ej. San Victorino + 4 vecinas a <700m en el centro, suma 733,829) generan un brillo combinado mayor que un portal aislado con más demanda individual pero sin vecinos cerca. El mapa por franja colorea cada estación por su propio valor, sin sumar vecinas, por eso nunca coincide. Se agregó `demanda_zona_700m` como variable candidata adicional (demanda propia + de estaciones a <700m), porque responde directo a la pregunta de estudio: hay zonas (no solo estaciones individuales) que acumulan presión y necesitarían coordinar varios puntos de refuerzo.
- **Ranking de días (pedido explícito del usuario):** entre los 10 hábiles típicos hay ~10% de variación real (no ruido). El día más cargado de todos (16-sep) coincide con el único atípico real detectado en el boxplot (pico nocturno 10-11pm) — posible mismo evento, hipótesis para el notebook 05.
- **Agosto vs. septiembre, probado con estadística, no solo mirado (pedido explícito del usuario):** t-test independiente, diferencia de medias 3.3%, **p=0.030 (sí es significativo)**. Pero esto NO prueba estacionalidad: los 5 días de cada semana no son observaciones independientes del mes, son 5 días de UNA sola semana (pseudo-replicación) — un efecto puntual de esa semana se repite en los 5 días e infla la "muestra" artificialmente. Para probar estacionalidad real haría falta comparar varias semanas de cada mes, de años distintos. Gráfica real en `docs/figs/agosto_vs_septiembre_habiles.png`.
- **Tabla explícita de validaciones por franja horaria (top 20 estaciones)**, pedida por el usuario para complementar el mapa visual — `data/processed/demanda_por_franja_top20.csv`. Permitió resolver una segunda duda real: **Calle 100-Marketmedios** no es un caso de "vecinas sumando brillo" (solo tiene 1 vecina a <700m) sino de **concentración temporal pura**: 317 validaciones en madrugada, pero 236,194 en pico PM — la cifra más alta de las 20 estaciones en esa franja específica (más que Portal Norte). Es una estación "especialista en salida de tarde" (perfil oficinas/comercio), no "generalista" como los portales.

### Sección 2.13 del notebook 02 — 6 mejoras al EDA (1-oct, a partir de revisar un CLAUDE.md de otra sesión)

El usuario encontró un plan de EDA más detallado en otra sesión (nunca llegó a bajar datos reales) y pidió incorporar lo valioso sin cambiar de rumbo. Se adoptaron 6 mejoras, todas con datos que ya se tenían (se descartó explícitamente el orden de estaciones en la troncal y el origen-destino por tarjeta, por costo/tiempo frente al 10 de octubre):

1. **Perfiles de usuario** (`Nombre_Perfil`, no usado hasta ahora): 50.7% Adulto, 29.7% Anonymous (tarjeta no personalizada), 8.2% Adulto Mayor. Nuevas variables `pct_adulto_mayor` y `pct_anonymous` agregadas a `estacion_features.parquet` — **hallazgo que no se esperaba**: las estaciones más "residenciales" en razón AM/PM (ej. Terreros, 10.6x) tienen MENOS % de adulto mayor, no más; correlación débil (-0.31), es una dimensión genuinamente nueva.
2. **Curva de Lorenz** (el gráfico, ya se tenía el Gini=0.484 pero no el gráfico).
3. **Heatmap estación × hora clásico** (estaba en el plan original, nunca se había construido) — excluye estaciones con <1000 validaciones/14 días para no distorsionar la escala.
4. **Dispersión % pico AM vs. % pico PM** — visualiza la correlación -0.92 ya conocida.
5. **Curvas de 5 estaciones representativas** (no las más grandes, 5 arquetipos distintos) superpuestas en un solo gráfico.
6. **Puntos críticos por percentil propio de cada estación** (p95 de sus propias 24h, no mezclando estaciones): 294 combinaciones estación-hora, 146 de 150 estaciones tienen exactamente 2 (su pico AM y su pico PM) — confirma el patrón de doble pico como la norma.

### Tercera restructuración del notebook 02 (1-oct): numeración plana tipo "pasos", igual al otro CLAUDE.md

El usuario pidió abandonar el esquema "2.1, 2.2, 2.3..." (que no cuenta una historia) y usar una lista plana de pasos con títulos simples, como en el otro documento ("1. Calidad e inventario, 2. Volumen y concentración, 3. Patrón temporal..."). Se adoptó esa numeración y se reordenó el contenido real para que la secuencia tenga sentido narrativo:

1. Contexto · 2. Construcción de la tabla base · 3. Calidad de datos · 4. Cruce geoespacial · 5. Patrón temporal del sistema · **6. Hábil vs. fin de semana vs. festivo** (movido antes, porque el paso 9 depende de esta distinción) · 7. Volumen y concentración (+ Lorenz, movida aquí desde "profundizando") · 8. Forma de la demanda por estación (+ heatmap, dispersión AM/PM, curvas representativas, movidas aquí) · 9. Series de tiempo: estacionariedad y descomposición · 10. Cómo se mueve la demanda en el espacio · 11. Puntos críticos estación-hora (antes enterrado en "2.11.6") · 12. Perfiles de usuario · 13. Variables candidatas para el modelo · 14. Conclusión final · 15. Apéndice.

Se corrigió también un bug de referencias previo a esta sesión: una tabla decía que "puntos críticos" era la sección 2.12 (variables candidatas) cuando siempre fue 2.11.6 — ya apunta correctamente a la sección 11.

**Regla para el futuro:** si se vuelve a reordenar el notebook, buscar todas las referencias cruzadas con el patrón `2\.(?:1[0-4]|[1-9])(?!\d)(?:\.\d+)?` (evita falsos positivos como el valor "2.07") antes de dar por terminado el cambio.

### Segunda restructuración del notebook 02 (1-oct, tras ver la figura por franja todavía con 14 días)

El usuario señaló 3 problemas más: (1) el mapa por franja horaria seguía usando los 14 días típicos, no solo hábiles; (2) faltaba claridad sobre qué ventana de fechas usa cada sección; (3) la estructura era confusa (las "conclusiones" aparecían antes de más análisis) y el cierre era solo un gráfico básico.

- **Mapa por franja horaria (2.9) reconstruido con solo los 10 días hábiles**, consistente con 2.8. El título de la figura ahora lo dice explícitamente ("SOLO días hábiles").
- **Tabla de ventanas de fechas agregada en 2.1**: qué sección usa 21 días / 14 días típicos / solo hábiles / solo la semana del festivo, y por qué.
- **Callout de "Objetivo + Datos usados" al inicio de cada sección** (2.2 a 2.10), para que no haya ambigüedad sobre qué pregunta responde cada una y con qué ventana de fechas.
- **Reordenado:** la sección "2.13 Profundizando el EDA" (perfiles de usuario, Lorenz, heatmap, dispersión, curvas representativas, puntos críticos) pasó a ser **2.11**, ubicada ANTES de "2.12 Variables candidatas" (que incluye el resumen de hallazgos) — antes el orden era ilógico (conclusiones de variables antes de análisis que las sustentan).
- **Conclusión final (ahora 2.13) sustancialmente más robusta**, ya no es solo un gráfico: agrega (a) una segunda mirada por zonas densas (La Campiña, Perdomo — demanda individual modesta pero zona comparable a un portal), (b) tabla de validación cruzada de las 3 hipótesis de `CLAUDE.md`, (c) tabla de qué sección responde cada una de las 5 preguntas específicas, (d) recomendación operativa final en prosa con limitaciones explícitas.
- **Información de contexto adicional** (antes 2.11) pasó al final como apéndice (2.14).

### Restructuración final del notebook 02 (1-oct, pedido explícito del usuario)

El usuario pidió 3 cosas: (1) quitar el ruido de "estaciones raras" (corrales de bicicletas, paraderos temporales) que venía apareciendo suelto en varias figuras, (2) rehacer la sección de atípicos/rigor estadístico usando **solo días hábiles** desde el inicio (evitar mezclar tipos de día y tener que explicar después por qué 24/26 candidatos eran falsos positivos), y (3) un **hilo conductor claro** que termine en una respuesta concreta: qué estaciones y horas necesitan personal de apoyo.

- **Hilo conductor agregado al inicio del notebook 02**, explicando el recorrido completo y la decisión de excluir instalaciones sin volumen real (demanda total < 1,000 validaciones/14 días) de las tablas de priorización.
- **Sección 2.8 reescrita con solo hábiles (10 días):** boxplot de outliers pasó de 26 candidatos (24 falsos, por mezclar tipos de día) a 9 candidatos, de los cuales solo 1 es real (16-sep, mismo hallazgo de antes, pero limpio desde el primer intento). **Cambio importante en el resultado de Dickey-Fuller:** con la serie mezclada daba p=0.22 (no estacionaria); aislando solo hábiles, la serie ya es estacionaria por sí misma (p≈0) — confirma formalmente que el quiebre fuerte en los datos es hábil vs. no-hábil, no una deriva gradual.
- **Sección 2.14 (nueva, el cierre del notebook):** combina puntos críticos (percentil 95 propio, solo hábiles — ahora **las 149 estaciones con volumen real tienen exactamente 2 horas críticas cada una**, sin excepciones) + franja horaria + zona (`demanda_zona_700m`) en una tabla de prioridad. **Resultado: el 100% de las horas críticas en día hábil caen en pico AM (132) o pico PM (166) — ninguna en mediodía, madrugada o noche.** Top 5 AM = puros portales (control de entrada); top 5 PM = mezcla de oficinas/cultura/comercio, incluyendo San Victorino (zona densa, coordinar varios puntos). Es la respuesta directa a la pregunta específica 5, con la limitación honesta de que es un ranking descriptivo del EDA, no el modelo formal (eso es el notebook 04).

### Corrección de encoding (1-oct, encontrada al construir el heatmap estación×hora)

El archivo es **UTF-8**, no latin1 como se documentó desde el notebook 01. Verificado con los bytes crudos (`\xc3\xa9` = "é" en UTF-8, confirmado con `ord()` en Python — sin depender de cómo lo muestra una terminal, que fue la fuente real de la confusión inicial: una terminal puede mostrar mal un dato correcto). `encoding='latin1'` partía cada tilde en 2 caracteres (ej. "América" → "AmÃ©rica"). Corregido en notebooks 01 y 02, y reconstruidos `fact_demanda_estacion_hora.parquet` y `estacion_features.parquet`.
