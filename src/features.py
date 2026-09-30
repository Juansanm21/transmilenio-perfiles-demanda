"""Construccion de variables (features) por estacion a partir de las validaciones.

Punto de la rubrica: b (construir la variable objetivo y las caracteristicas).

Cada funcion recibe un DataFrame (pandas o dask) con, al menos, las columnas
Estacion_Parada y Fecha_Transaccion, y devuelve una tabla agregada a nivel
de estacion. Implementar aqui, no directamente en los notebooks, para que
el flujo de Prefect (src/flows) pueda reutilizar el mismo codigo.
"""


def construir_features(df):
    """Devuelve una tabla estacion -> features (demanda total, % pico AM/PM,
    razon AM/PM, razon fin de semana/habil, curva horaria normalizada de 24
    valores, indice de concentracion en pico). Ver notebooks/02_eda.ipynb.
    """
    raise NotImplementedError
