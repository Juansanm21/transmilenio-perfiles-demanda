"""Flujo de Prefect: pipeline_transmilenio.

Punto de la rubrica: d. Debe ser recurrente e idempotente: si llega una
semana nueva de datos, el flujo la transforma y rehace la analitica sola.

Tareas (ver CLAUDE.md seccion 5):
    1. detectar_archivos_nuevos
    2. validar_esquema (Pandera)
    3. limpiar_y_convertir (-> Parquet)
    4. agregar_con_dask
    5. construir_features
    6. asignar_clusters (carga modelo entrenado)
    7. detectar_anomalias
    8. calcular_kpis
    9. cargar_a_rds (upsert)
    10. registrar_ejecucion (tabla etl_log)
"""
from prefect import flow


@flow(name="pipeline_transmilenio")
def pipeline_transmilenio(carpeta_datos: str, reentrenar: bool = False):
    raise NotImplementedError
