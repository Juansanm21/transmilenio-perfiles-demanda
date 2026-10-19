"""Dash + Datashader: visualización de las ~31,5 M validaciones troncales de TransMilenio.

Estructura del proyecto (la misma del laboratorio de clase):
    app.py               crea la aplicación, la barra de navegación y arranca el servidor
    configuracion.toml   parámetros editables (rutas de los datos, detalle del mapa, puerto)
    servicios/           datos: preparación de puntos, repositorio en memoria, servidor de teselas
    components/          piezas visuales reutilizables (tarjetas KPI, filtros, mapa)
    pages/               una página por archivo; Dash las registra automáticamente
    assets/              CSS que Dash carga solo
"""
import dash
from dash import Dash
import dash_bootstrap_components as dbc

from servicios import contexto

configuracion = contexto.configuracion()
contexto.repositorio()  # carga los datos antes de importar las páginas

app = Dash(__name__, use_pages=True, external_stylesheets=[dbc.themes.FLATLY],
           title="TransMilenio · Dash + Datashader")
server = app.server  # servidor Flask: lo usa gunicorn en producción
contexto.servidor_teselas().registrar_ruta(server, app.config.requests_pathname_prefix)

barra_navegacion = dbc.NavbarSimple(
    [dbc.NavItem(dbc.NavLink(pagina["name"], href=pagina["relative_path"]))
     for pagina in sorted(dash.page_registry.values(), key=lambda pagina: pagina["order"])],
    brand="Demanda troncal TransMilenio · Big Data", color="danger", dark=True, className="mb-3",
)

app.layout = dbc.Container([barra_navegacion, dash.page_container], fluid=True, className="dbc")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=configuracion.puerto, debug=configuracion.depuracion)
