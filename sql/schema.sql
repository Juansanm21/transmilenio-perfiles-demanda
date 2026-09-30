-- Esquema de la base de resultados en AWS RDS (PostgreSQL).
-- Punto de la rubrica: f. Solo tablas de RESULTADOS del proceso analitico,
-- nunca el dataset original de validaciones.

CREATE TABLE IF NOT EXISTS dim_estacion (
    id_estacion     TEXT PRIMARY KEY,
    nombre          TEXT NOT NULL,
    troncal         TEXT,
    latitud         DOUBLE PRECISION,
    longitud        DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS fact_demanda_estacion_hora (
    id_estacion     TEXT REFERENCES dim_estacion(id_estacion),
    fecha           DATE NOT NULL,
    hora            SMALLINT NOT NULL,
    tipo_dia        TEXT,              -- habil / sabado / domingo / festivo
    validaciones    INTEGER NOT NULL,
    PRIMARY KEY (id_estacion, fecha, hora)
);

CREATE TABLE IF NOT EXISTS estacion_perfil (
    id_estacion         TEXT PRIMARY KEY REFERENCES dim_estacion(id_estacion),
    cluster             INTEGER,
    demanda_total       BIGINT,
    pct_pico_am         DOUBLE PRECISION,
    pct_pico_pm         DOUBLE PRECISION,
    razon_am_pm         DOUBLE PRECISION,
    razon_finde_habil   DOUBLE PRECISION,
    indice_concentracion DOUBLE PRECISION,
    indice_prioridad    DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS cluster_resumen (
    cluster         INTEGER PRIMARY KEY,
    num_estaciones  INTEGER,
    nombre_perfil   TEXT,              -- ej. "residencial", "destino laboral"
    curva_horaria   DOUBLE PRECISION[] -- 24 valores promedio normalizados
);

CREATE TABLE IF NOT EXISTS anomalias (
    id_estacion     TEXT REFERENCES dim_estacion(id_estacion),
    fecha           DATE NOT NULL,
    hora            SMALLINT NOT NULL,
    observado       INTEGER,
    esperado        DOUBLE PRECISION,
    score           DOUBLE PRECISION,
    PRIMARY KEY (id_estacion, fecha, hora)
);

CREATE TABLE IF NOT EXISTS kpi_diario (
    fecha           DATE NOT NULL,
    troncal         TEXT NOT NULL,
    validaciones_totales INTEGER,
    PRIMARY KEY (fecha, troncal)
);

CREATE TABLE IF NOT EXISTS etl_log (
    id              SERIAL PRIMARY KEY,
    ejecutado_en    TIMESTAMP NOT NULL DEFAULT now(),
    archivos_procesados TEXT[],
    filas_procesadas    BIGINT,
    estado          TEXT,   -- ok / error
    detalle         TEXT
);

-- Usuario de solo lectura para Grafana (crear aparte del usuario de escritura del ETL):
-- CREATE ROLE grafana_ro WITH LOGIN PASSWORD '...';
-- GRANT CONNECT ON DATABASE transmilenio TO grafana_ro;
-- GRANT USAGE ON SCHEMA public TO grafana_ro;
-- GRANT SELECT ON ALL TABLES IN SCHEMA public TO grafana_ro;
