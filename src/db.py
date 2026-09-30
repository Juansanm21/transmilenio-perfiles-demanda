"""Conexion y carga de resultados a PostgreSQL en AWS RDS.

Punto de la rubrica: f. SOLO se cargan aqui tablas de RESULTADOS
(features por estacion, clusters, anomalias, KPIs) -- nunca el
dataset original de validaciones.

Credenciales se leen de variables de entorno (.env, ver .env.example),
nunca hardcodeadas.
"""
import os


def get_engine():
    """Crea el engine de SQLAlchemy a partir de las variables de entorno
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD.
    """
    raise NotImplementedError
