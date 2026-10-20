"""Flujo Prefect: de los ZIP diarios de validaciones a la tabla de análisis en AWS (PostgreSQL RDS).

Cada vez que corre:
  1. Lista los ZIP de la carpeta y valida que traigan las columnas esperadas.
  2. Cuenta con Dask las validaciones por fecha, estación y hora (solo los ZIP que aún no están en la caché).
  3. Busca semanas completas (7 días con más de 10.000 validaciones) que todavía no estén cargadas.
  4. Calcula, para cada semana nueva, el perfil horario y el ranking de estaciones por franja.
  5. Carga el resultado en UNA tabla de AWS (`analisis_estacion_hora`) con upsert: si se repite, no duplica.

Si no hay una semana completa nueva, termina sin hacer nada. A AWS solo llegan resultados agregados.

Uso (desde la carpeta del proyecto, con el venv activo):
    python src/pipeline_transmilenio.py                          # una corrida
    python src/pipeline_transmilenio.py --fecha-corte 2026-08-09 # simula que solo han llegado los archivos hasta esa fecha
    python src/pipeline_transmilenio.py --servir                 # deja el flujo programado (cron diario)
"""
import argparse
import os
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import dask
import numpy as np
import pandas as pd
import pandera.pandas as pa
from dotenv import load_dotenv
from prefect import flow, get_run_logger, task
from sqlalchemy import Boolean, Column, Date, DateTime, Float, Integer, MetaData, SmallInteger, String, Table, Text, create_engine, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import URL

# ----------------------------------------------------------------------------------------------------------------------
# Configuración (rutas relativas al proyecto; las credenciales salen de .env, que no se sube a GitHub)
# ----------------------------------------------------------------------------------------------------------------------
RAIZ = Path(__file__).resolve().parents[1]
load_dotenv(RAIZ / ".env")

RAW_VALIDACIONES = RAIZ / "data" / "raw" / "validaciones_troncal"
RAW_ESTACIONES = RAIZ / "data" / "raw" / "estaciones"
CACHE_DIA = RAIZ / "data" / "processed" / "panel_dia"

MIN_VALIDACIONES_DIA = 10_000          # un día con menos validaciones es una cola de medianoche, no un día completo
UMBRAL = 1_000                         # validaciones por hora a partir de las cuales una estación está en alta presión
CAPACIDAD_PERSONA = 1_500              # validaciones por hora que una persona puede ordenar (supuesto)
FRANJAS = {"mañana": (5, 8), "tarde": (16, 19)}
FRACCION_VENTANA = 0.5                 # la ventana de refuerzo son las horas con al menos 50 % del pico de la estación
TIPO_DIA_BASE = "hábil"

FESTIVOS = {   # editar cuando llegue un periodo nuevo
    date(2026, 8, 7), date(2026, 8, 17), date(2026, 10, 12), date(2026, 11, 2), date(2026, 11, 16), date(2026, 12, 8), date(2026, 12, 25),
}

COLUMNAS = ["Fecha_Transaccion", "Estacion_Parada"]
ESQUEMA_MUESTRA = pa.DataFrameSchema({
    "Fecha_Transaccion": pa.Column(str, pa.Check.str_matches(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}"), nullable=False),
    "Estacion_Parada": pa.Column(str, pa.Check.str_matches(r"^\(\d{5}\) .+"), nullable=False),
})

# ----------------------------------------------------------------------------------------------------------------------
# Tabla de AWS (la única): una fila por semana x estación x hora, con el análisis incluido
# ----------------------------------------------------------------------------------------------------------------------
METADATA = MetaData()
TABLA = Table(
    "analisis_estacion_hora", METADATA,
    Column("semana_inicio", Date, primary_key=True),
    Column("cod_estacion", String(5), primary_key=True),
    Column("hora", SmallInteger, primary_key=True),
    Column("nombre_estacion", Text), Column("troncal", Text), Column("latitud", Float), Column("longitud", Float),
    Column("tipo_dia_base", Text), Column("dias_usados", SmallInteger),
    Column("validaciones_promedio", Float),
    Column("franja", Text), Column("exceso_sobre_umbral", Float), Column("en_ventana_refuerzo", Boolean),
    Column("pico_franja", Integer), Column("hora_pico", SmallInteger),
    Column("carga_sobre_umbral", Float), Column("personas_pico", SmallInteger), Column("puesto", SmallInteger),
    Column("umbral", Float), Column("capacidad_persona", Float),
    Column("fecha_carga", DateTime(timezone=True), server_default=func.now()),
)
LLAVE = ["semana_inicio", "cod_estacion", "hora"]

ESQUEMA_RESULTADO = pa.DataFrameSchema(
    {
        "cod_estacion": pa.Column(str, pa.Check.str_matches(r"^\d{5}$")),
        "hora": pa.Column(int, pa.Check.in_range(0, 23)),
        "validaciones_promedio": pa.Column(float, pa.Check.ge(0)),
    },
    unique=LLAVE, coerce=True,
)


def crear_engine():
    """Conexión a PostgreSQL con los datos de .env (DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, DB_SSLMODE)."""
    faltan = [k for k in ("DB_HOST", "DB_NAME", "DB_USER", "DB_PASSWORD") if not os.getenv(k)]
    if faltan:
        raise RuntimeError(f"Faltan variables en .env: {faltan} (ver .env.example)")
    url = URL.create("postgresql+psycopg2", username=os.environ["DB_USER"], password=os.environ["DB_PASSWORD"],
                     host=os.environ["DB_HOST"], port=int(os.getenv("DB_PORT", "5432")), database=os.environ["DB_NAME"])
    return create_engine(url, connect_args={"sslmode": os.getenv("DB_SSLMODE", "require")}, pool_pre_ping=True)


# ----------------------------------------------------------------------------------------------------------------------
# Calendario
# ----------------------------------------------------------------------------------------------------------------------
def tipo_dia(fecha: date) -> str:
    """festivo, sábado, domingo o hábil, construido desde la fecha."""
    if fecha in FESTIVOS:
        return "festivo"
    return {5: "sábado", 6: "domingo"}.get(fecha.weekday(), "hábil")


def lunes_de(fecha: date) -> date:
    return fecha - timedelta(days=fecha.weekday())


def semanas_completas(demanda: pd.DataFrame) -> list[date]:
    """Lunes de las semanas con sus 7 días completos (más de MIN_VALIDACIONES_DIA validaciones cada uno)."""
    totales = demanda.groupby("fecha")["validaciones"].sum()
    completos = [date.fromisoformat(f) for f in totales[totales > MIN_VALIDACIONES_DIA].index]
    por_semana: dict[date, int] = {}
    for f in completos:
        por_semana[lunes_de(f)] = por_semana.get(lunes_de(f), 0) + 1
    return sorted(lunes for lunes, n in por_semana.items() if n == 7)


# ----------------------------------------------------------------------------------------------------------------------
# Del ZIP al conteo por fecha, estación y hora
# ----------------------------------------------------------------------------------------------------------------------
def fecha_del_archivo(ruta: Path) -> date:
    """Fecha en el nombre validacionTroncalAAAAMMDD.zip."""
    s = Path(ruta).stem[-8:]
    return date(int(s[:4]), int(s[4:6]), int(s[6:]))


def validar_esquema(ruta: Path, filas: int = 5_000) -> dict:
    """Comprueba que el ZIP traiga las columnas necesarias y que sus valores tengan el formato esperado."""
    encabezado = pd.read_csv(ruta, compression="zip", encoding="utf-8", nrows=0).columns
    faltan = [c for c in COLUMNAS if c not in encabezado]
    if faltan:
        raise ValueError(f"{Path(ruta).name}: faltan las columnas {faltan}")
    muestra = pd.read_csv(ruta, compression="zip", encoding="utf-8", usecols=COLUMNAS, nrows=filas, dtype=str)
    ESQUEMA_MUESTRA.validate(muestra)
    return {"archivo": Path(ruta).name, "columnas": len(encabezado)}


def agregar_dia(ruta: Path, trozo: int = 400_000) -> dict:
    """Cuenta validaciones por (fecha real, estación, hora). Lee solo dos columnas y por trozos, así que no carga el archivo completo."""
    conteos, nombres, filas = [], [], 0
    for df in pd.read_csv(ruta, compression="zip", encoding="utf-8", usecols=COLUMNAS, chunksize=trozo, dtype=str):
        df = df.dropna()
        filas += len(df)
        ft, est = df["Fecha_Transaccion"], df["Estacion_Parada"]
        claves = pd.DataFrame({"fecha": ft.str[:10], "cod_estacion": est.str[1:6], "hora": ft.str[11:13].astype("int16")})
        conteos.append(claves.groupby(["fecha", "cod_estacion", "hora"]).size())
        nombres.append(est.value_counts())
    demanda = pd.concat(conteos).groupby(level=[0, 1, 2]).sum().rename("validaciones").reset_index()
    nom = pd.concat(nombres).groupby(level=0).sum().rename("n").reset_index().rename(columns={"Estacion_Parada": "crudo"})
    nom["cod_estacion"], nom["nombre"] = nom["crudo"].str[1:6], nom["crudo"].str[8:]
    if int(demanda["validaciones"].sum()) != filas:
        raise ValueError(f"{Path(ruta).name}: el conteo ({demanda['validaciones'].sum()}) no coincide con las filas leídas ({filas})")
    return {"demanda": demanda, "nombres": nom[["cod_estacion", "nombre", "n"]], "filas": filas}


def agregar_dia_con_cache(ruta: str, cache: str = str(CACHE_DIA), forzar: bool = False) -> dict:
    """Agrega un ZIP y guarda el resultado en la caché (un Parquet pequeño por día). Si ya está, no lo vuelve a leer."""
    ruta, cache = Path(ruta), Path(cache)
    cache.mkdir(parents=True, exist_ok=True)
    sello = ruta.stem[-8:]
    f_dem, f_nom = cache / f"demanda_{sello}.parquet", cache / f"nombres_{sello}.parquet"
    if f_dem.exists() and f_nom.exists() and not forzar:
        return {"archivo": ruta.name, "filas": int(pd.read_parquet(f_dem, columns=["validaciones"])["validaciones"].sum()), "segundos": 0.0, "desde_cache": True}
    t0 = time.perf_counter()
    res = agregar_dia(ruta)
    res["demanda"].to_parquet(f_dem, index=False)
    res["nombres"].to_parquet(f_nom, index=False)
    return {"archivo": ruta.name, "filas": res["filas"], "segundos": round(time.perf_counter() - t0, 1), "desde_cache": False}


def procesar_con_dask(rutas: list[Path], n_workers: int, memoria: str, forzar: bool = False) -> list[dict]:
    """Agrega los archivos en paralelo con un LocalCluster de Dask (un archivo por worker). Workers y memoria son parámetros del flujo."""
    if not rutas:
        return []
    if n_workers <= 1:
        return [agregar_dia_con_cache(str(r), forzar=forzar) for r in rutas]
    from dask.distributed import Client, LocalCluster

    with LocalCluster(n_workers=n_workers, threads_per_worker=1, memory_limit=memoria, processes=True) as cluster, Client(cluster):
        return list(dask.compute(*[dask.delayed(agregar_dia_con_cache)(str(r), forzar=forzar) for r in rutas]))


def leer_cache(hasta: date, cache: Path = CACHE_DIA) -> tuple[pd.DataFrame, pd.Series]:
    """Une la caché de los archivos con fecha <= `hasta`: demanda por (fecha, estación, hora) y un nombre por estación."""
    archivos = lambda prefijo: [f for f in sorted(Path(cache).glob(f"{prefijo}_*.parquet")) if fecha_del_archivo(f) <= hasta]
    demanda = pd.concat([pd.read_parquet(f) for f in archivos("demanda")], ignore_index=True)
    demanda = demanda.groupby(["fecha", "cod_estacion", "hora"], as_index=False)["validaciones"].sum()   # una fecha puede venir en dos archivos
    nombres = pd.concat([pd.read_parquet(f) for f in archivos("nombres")], ignore_index=True)
    nombres = nombres.groupby(["cod_estacion", "nombre"], as_index=False)["n"].sum().sort_values("n", ascending=False)
    return demanda, nombres.drop_duplicates("cod_estacion").set_index("cod_estacion")["nombre"]   # un código con dos nombres: el más frecuente


def estaciones_referencia() -> pd.DataFrame:
    """Troncal y coordenadas de las estaciones desde los archivos oficiales (si faltan, quedan vacías)."""
    try:
        oficial = pd.read_csv(RAW_ESTACIONES / "estaciones_troncales.csv", dtype=str, encoding="utf-8-sig").set_index("numero_estacion")
        ui = pd.read_csv(RAW_ESTACIONES / "estaciones_troncales_ui.csv", dtype=str, encoding="utf-8-sig").set_index("num_est")
    except FileNotFoundError:
        return pd.DataFrame(columns=["troncal", "latitud", "longitud"])
    idx = oficial.index.union(ui.index)
    ref = pd.DataFrame(index=idx)
    ref["troncal"] = oficial["troncal_estacion"].reindex(idx)
    ref["latitud"] = pd.to_numeric(oficial["latitud_estacion"].reindex(idx)).fillna(pd.to_numeric(ui["latitud"].reindex(idx)))
    ref["longitud"] = pd.to_numeric(oficial["longitud_estacion"].reindex(idx)).fillna(pd.to_numeric(ui["longitud"].reindex(idx)))
    return ref


# ----------------------------------------------------------------------------------------------------------------------
# El análisis de una semana
# ----------------------------------------------------------------------------------------------------------------------
def analizar_semana(demanda: pd.DataFrame, nombres: pd.Series, ref: pd.DataFrame, lunes: date, tipo_base: str = TIPO_DIA_BASE,
                    umbral: float = UMBRAL, capacidad: float = CAPACIDAD_PERSONA, fraccion: float = FRACCION_VENTANA) -> pd.DataFrame:
    """Tabla de la semana: una fila por estación y hora con la demanda promedio y el análisis de refuerzo.

    - validaciones_promedio: promedio de los días de la semana del tipo de día base (las horas sin validaciones cuentan como 0).
    - exceso_sobre_umbral: lo que esa hora excede el umbral.
    - Por estación y franja (mañana, tarde): pico, hora del pico, carga sobre el umbral (suma de excesos), personas en el pico
      (pico / capacidad por persona, hacia arriba) y puesto en el ranking de la franja. Se repiten en las horas de la franja.
    - en_ventana_refuerzo: horas de la franja con al menos `fraccion` del pico de la estación (y no menos que el umbral).
    """
    dias = [lunes + timedelta(days=k) for k in range(7)]
    base = [d for d in dias if tipo_dia(d) == tipo_base]
    if not base:
        return pd.DataFrame()
    fechas = [d.isoformat() for d in base]
    d = demanda[demanda["fecha"].isin(fechas)]
    idx = pd.MultiIndex.from_product([sorted(d["cod_estacion"].unique()), fechas, range(24)], names=["cod_estacion", "fecha", "hora"])
    cubo = d.set_index(["cod_estacion", "fecha", "hora"])["validaciones"].reindex(idx, fill_value=0)
    perfil = cubo.groupby(level=["cod_estacion", "hora"]).mean().unstack()                    # filas: estación, columnas: hora 0-23

    tabla = perfil.stack().rename("validaciones_promedio").reset_index()
    tabla["exceso_sobre_umbral"] = (tabla["validaciones_promedio"] - umbral).clip(lower=0)

    partes = []
    for franja, (a, z) in FRANJAS.items():
        sub = perfil.loc[:, a:z]
        pico = sub.max(axis=1)
        alta = pico >= umbral
        en_ventana = sub.ge(np.maximum(umbral, fraccion * pico), axis=0) & alta.to_numpy()[:, None]
        resumen = pd.DataFrame({"franja": franja, "pico_franja": pico.round(), "hora_pico": sub.idxmax(axis=1),
                                "carga_sobre_umbral": (sub - umbral).clip(lower=0).sum(axis=1).round(),
                                "personas_pico": np.ceil(pico / capacidad).where(alta)})
        resumen["puesto"] = resumen["carga_sobre_umbral"].where(alta).rank(ascending=False, method="first")
        horas = en_ventana.stack().rename("en_ventana_refuerzo").reset_index()
        partes.append(horas.merge(resumen.reset_index(), on="cod_estacion"))
    analisis = pd.concat(partes, ignore_index=True)

    tabla = tabla.merge(analisis, on=["cod_estacion", "hora"], how="left")
    tabla["en_ventana_refuerzo"] = tabla["en_ventana_refuerzo"].fillna(False).astype(bool)
    tabla["nombre_estacion"] = tabla["cod_estacion"].map(nombres)
    tabla = tabla.join(ref.reindex(tabla["cod_estacion"]).reset_index(drop=True)[["troncal", "latitud", "longitud"]])
    tabla.insert(0, "semana_inicio", lunes)
    tabla["tipo_dia_base"], tabla["dias_usados"] = tipo_base, len(base)
    tabla["umbral"], tabla["capacidad_persona"] = float(umbral), float(capacidad)
    tabla["validaciones_promedio"] = tabla["validaciones_promedio"].round(1)
    for c in ("hora_pico", "personas_pico", "puesto"):
        tabla[c] = tabla[c].astype("Int64")
    columnas = [c.name for c in TABLA.columns if c.name != "fecha_carga"]
    return ESQUEMA_RESULTADO.validate(tabla[columnas])


# ----------------------------------------------------------------------------------------------------------------------
# Flujo de Prefect
# ----------------------------------------------------------------------------------------------------------------------
@task(name="detectar_archivos")
def detectar_archivos(carpeta: str, fecha_corte: date) -> list[Path]:
    """ZIP de la carpeta con fecha <= fecha_corte (la fecha de corte simula qué archivos han llegado)."""
    rutas = sorted(r for r in Path(carpeta).glob("validacionTroncal*.zip") if fecha_del_archivo(r) <= fecha_corte)
    get_run_logger().info(f"{len(rutas)} archivos en {carpeta} hasta {fecha_corte}")
    return rutas


@task(name="validar_esquema")
def validar_archivos(rutas: list[Path], forzar: bool) -> list[Path]:
    """Valida solo los archivos que aún no están en la caché. Devuelve los archivos que hay que procesar."""
    nuevos = [r for r in rutas if forzar or not (CACHE_DIA / f"demanda_{r.stem[-8:]}.parquet").exists()]
    for r in nuevos:
        validar_esquema(r)
    get_run_logger().info(f"{len(nuevos)} archivos nuevos validados; {len(rutas) - len(nuevos)} ya estaban en la caché")
    return nuevos


@task(name="agregar_con_dask")
def agregar_con_dask(nuevos: list[Path], n_workers: int, memoria: str, forzar: bool) -> list[dict]:
    t0 = time.perf_counter()
    res = procesar_con_dask(nuevos, n_workers, memoria, forzar)
    get_run_logger().info(f"Dask ({n_workers} workers, {memoria} por worker): {len(res)} archivos, "
                          f"{sum(r['filas'] for r in res):,} validaciones en {time.perf_counter() - t0:.1f} s".replace(",", "."))
    return res


@task(name="semanas_pendientes", retries=2, retry_delay_seconds=10)
def semanas_pendientes(fecha_corte: date, forzar: bool) -> list[date]:
    """Semanas completas en la caché que todavía no están en la tabla de AWS (la tabla se crea si no existe)."""
    engine = crear_engine()
    METADATA.create_all(engine)
    with engine.connect() as con:
        cargadas = {f[0] for f in con.execute(select(TABLA.c.semana_inicio).distinct())}
    demanda, _ = leer_cache(fecha_corte)
    completas = semanas_completas(demanda)
    pendientes = [s for s in completas if forzar or s not in cargadas]
    get_run_logger().info(f"Semanas completas: {[s.isoformat() for s in completas]} | ya cargadas: {sorted(s.isoformat() for s in cargadas)} | pendientes: {[s.isoformat() for s in pendientes]}")
    return pendientes


@task(name="calcular_analisis")
def calcular_analisis(semana: date, fecha_corte: date, tipo_base: str, umbral: float, capacidad: float) -> pd.DataFrame:
    demanda, nombres = leer_cache(fecha_corte)
    tabla = analizar_semana(demanda, nombres, estaciones_referencia(), semana, tipo_base, umbral, capacidad)
    get_run_logger().info(f"Semana {semana}: {len(tabla)} filas, {tabla['cod_estacion'].nunique() if len(tabla) else 0} estaciones, {tabla['dias_usados'].iloc[0] if len(tabla) else 0} días de tipo {tipo_base}")
    return tabla


@task(name="cargar_a_aws", retries=2, retry_delay_seconds=15)
def cargar_a_aws(tabla: pd.DataFrame) -> int:
    """Upsert por (semana_inicio, cod_estacion, hora) en una sola transacción: la semana queda completa o no queda."""
    if tabla.empty:
        return 0
    registros = tabla.astype(object).where(tabla.notna(), None).to_dict("records")
    ahora = datetime.now(timezone.utc)
    for r in registros:
        r["fecha_carga"] = ahora
    engine = crear_engine()
    with engine.begin() as con:
        for i in range(0, len(registros), 500):          # un INSERT de varias filas por lote: pocas idas y vueltas a AWS
            stmt = insert(TABLA).values(registros[i:i + 500])
            stmt = stmt.on_conflict_do_update(index_elements=LLAVE, set_={c.name: stmt.excluded[c.name] for c in TABLA.columns if c.name not in LLAVE})
            con.execute(stmt)
    get_run_logger().info(f"Semana {tabla['semana_inicio'].iloc[0]}: {len(registros)} filas cargadas en {TABLA.name}")
    return len(registros)


@flow(name="pipeline_transmilenio", log_prints=True)
def pipeline_transmilenio(carpeta: str = str(RAW_VALIDACIONES), fecha_corte: str | None = None, forzar: bool = False,
                          tipo_dia_base: str = TIPO_DIA_BASE, umbral: float = UMBRAL, capacidad_persona: float = CAPACIDAD_PERSONA,
                          n_workers: int = 4, memoria: str = "2GB") -> dict:
    """Carga a AWS las semanas completas que aún no están. `fecha_corte` (AAAA-MM-DD) simula qué archivos han llegado; `forzar` recalcula."""
    corte = date.fromisoformat(fecha_corte) if fecha_corte else date.max
    rutas = detectar_archivos(carpeta, corte)
    nuevos = validar_archivos(rutas, forzar)
    agregar_con_dask(nuevos, n_workers, memoria, forzar)
    pendientes = semanas_pendientes(corte, forzar)
    if not pendientes:
        print("No hay semanas completas nuevas por cargar.")
        return {"semanas_cargadas": [], "filas": 0}
    filas = 0
    for semana in pendientes:
        filas += cargar_a_aws(calcular_analisis(semana, corte, tipo_dia_base, umbral, capacidad_persona))
    print(f"Listo: {len(pendientes)} semana(s) cargada(s), {filas} filas.")
    return {"semanas_cargadas": [s.isoformat() for s in pendientes], "filas": filas}


if __name__ == "__main__":      # la protección es obligatoria: Dask crea procesos que vuelven a importar este archivo
    ap = argparse.ArgumentParser(description="Flujo Prefect: ZIP de validaciones -> tabla de análisis en AWS.")
    ap.add_argument("--carpeta", default=str(RAW_VALIDACIONES))
    ap.add_argument("--fecha-corte", help="AAAA-MM-DD: solo se consideran los archivos hasta esa fecha")
    ap.add_argument("--forzar", action="store_true", help="vuelve a procesar y a cargar aunque ya esté")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--memoria", default="2GB", help="memoria por worker de Dask")
    ap.add_argument("--servir", action="store_true", help="deja el flujo programado en vez de correrlo una vez")
    ap.add_argument("--cron", default="0 6 * * *", help="programación con --servir (por defecto, todos los días a las 6:00)")
    a = ap.parse_args()
    if a.servir:
        pipeline_transmilenio.serve(name="pipeline-transmilenio", cron=a.cron,
                                    parameters={"carpeta": a.carpeta, "n_workers": a.workers, "memoria": a.memoria})
    else:
        pipeline_transmilenio(carpeta=a.carpeta, fecha_corte=a.fecha_corte, forzar=a.forzar, n_workers=a.workers, memoria=a.memoria)
