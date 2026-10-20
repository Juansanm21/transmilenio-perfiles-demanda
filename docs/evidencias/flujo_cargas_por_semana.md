# Evidencia: el flujo de Prefect carga las semanas completas que van llegando

Generado el 2026-10-19 21:01 desde la base de AWS (RDS PostgreSQL, tabla `analisis_estacion_hora`), con el usuario de solo lectura.

## 1. Qué hay hoy en la tabla

Consulta:

```sql
SELECT semana_inicio, COUNT(*) AS filas, COUNT(DISTINCT cod_estacion) AS estaciones, MAX(dias_usados) AS dias_habiles, MAX(fecha_carga) AS cargada_el FROM analisis_estacion_hora GROUP BY semana_inicio ORDER BY semana_inicio;
```

Resultado (10,776 filas en total):

```
semana_inicio  filas  estaciones  dias_habiles           cargada_el
   2026-08-03   3528         147             4 2026-10-20 00:07 UTC
   2026-08-10   3624         151             5 2026-10-20 00:14 UTC
   2026-09-14   3624         151             5 2026-10-20 00:14 UTC
```

La semana del 3 de agosto tiene 4 días hábiles porque el 7 de agosto fue festivo; las otras dos tienen 5.

## 2. Cómo llegaron las semanas (logs del flujo)

El flujo mira los ZIP de la carpeta, procesa solo los que aún no están en la caché y carga las semanas completas que todavía no están en la tabla. La carga es un *upsert* por (`semana_inicio`, `cod_estacion`, `hora`): si se repite, no duplica.

**Corrida 1: llega la primera semana** (`--fecha-corte 2026-08-09`, solo los archivos hasta el 9 de agosto):

```
19:07:43.207 | INFO    | Task run 'detectar_archivos-f02' - 7 archivos en <carpeta del proyecto>
19:07:43.219 | INFO    | Task run 'validar_esquema-3cd' - 0 archivos nuevos validados; 7 ya estaban en la caché
19:07:43.230 | INFO    | Task run 'agregar_con_dask-2a8' - Dask (4 workers, 2GB por worker): 0 archivos. 0 
19:07:46.203 | INFO    | Task run 'semanas_pendientes-1e8' - Semanas completas: ['2026-08-03'] | ya cargadas: [] | 
19:07:46.439 | INFO    | Task run 'calcular_analisis-6de' - Semana 2026-08-03: 3528 filas, 147 estaciones, 4 días de 
19:07:50.527 | INFO    | Task run 'cargar_a_aws-b60' - Semana 2026-08-03: 3528 filas cargadas en analisis_estacion_hora
19:07:50.534 | INFO    | Flow run 'devious-starfish' - Listo: 1 semana(s) cargada(s), 3528 filas.
```

**Corrida 2: llegan las semanas siguientes** (sin fecha de corte; el flujo procesa los 7 archivos nuevos de septiembre y carga la semana del 10 de agosto y la del 14 de septiembre):

```
19:13:22.015 | INFO    | Task run 'detectar_archivos-902' - 21 archivos en <carpeta del proyecto>
19:13:24.824 | INFO    | Task run 'validar_esquema-e13' - 7 archivos nuevos validados; 14 ya estaban en la caché
19:13:58.842 | INFO    | Task run 'agregar_con_dask-0ec' - Dask (4 workers, 2GB por worker): 7 archivos. 11.213.513 
19:14:01.417 | INFO    | Task run 'semanas_pendientes-066' - Semanas completas: ['2026-08-03', '2026-08-10', 
19:14:01.694 | INFO    | Task run 'calcular_analisis-4ad' - Semana 2026-08-10: 3624 filas, 151 estaciones, 5 días de 
19:14:05.655 | INFO    | Task run 'cargar_a_aws-882' - Semana 2026-08-10: 3624 filas cargadas en analisis_estacion_hora
19:14:05.929 | INFO    | Task run 'calcular_analisis-eee' - Semana 2026-09-14: 3624 filas, 151 estaciones, 5 días de 
19:14:09.671 | INFO    | Task run 'cargar_a_aws-c89' - Semana 2026-09-14: 3624 filas cargadas en analisis_estacion_hora
19:14:09.678 | INFO    | Flow run 'festive-bulldog' - Listo: 2 semana(s) cargada(s), 7248 filas.
```

**Corrida 3: no llega nada nuevo** (el flujo no duplica ni recalcula):

```
19:14:22.632 | INFO    | Task run 'detectar_archivos-e32' - 21 archivos en <carpeta del proyecto>
19:14:22.640 | INFO    | Task run 'validar_esquema-df3' - 0 archivos nuevos validados; 21 ya estaban en la caché
19:14:22.648 | INFO    | Task run 'agregar_con_dask-371' - Dask (4 workers, 2GB por worker): 0 archivos. 0 
19:14:25.423 | INFO    | Task run 'semanas_pendientes-0ea' - Semanas completas: ['2026-08-03', '2026-08-10', 
19:14:25.428 | INFO    | Flow run 'rigorous-limpet' - No hay semanas completas nuevas por cargar.
```

## 3. Cómo repetir la comprobación

1. Ejecutar la consulta de la sección 1 en DBeaver (o con cualquier cliente de PostgreSQL) y guardar la captura: muestra una fila por semana cargada.
2. Ejecutar el flujo: `python src/pipeline_transmilenio.py`. Si no hay semanas nuevas, el registro dice *No hay semanas completas nuevas por cargar*.
3. Para simular la llegada de una semana nueva, copiar un ZIP de una semana nueva a `data/raw/validaciones_troncal/` (o usar `--fecha-corte`) y volver a ejecutar el flujo: aparece una fila más en la consulta de la sección 1.
