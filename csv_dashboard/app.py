import pandas as pd
import plotly.express as px
from dash import Dash, html, dcc, Input, Output, dash_table
import dash_bootstrap_components as dbc
import os

app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.title = "Dashboard Lotería de Salta"
server = app.server

app._assets_folder = os.path.join(os.path.dirname(__file__), 'assets')
app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
        <style>
            h1, h2, h3, h4 {
                color: white;
            }
        </style>
    </head>
    <body style="background-color: #612482;">
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>
'''

# ================== CARGA DE DATASETS ==================
def cargar_csv_con_fechas(filename, date_col, dayfirst=False):
    if not os.path.exists(filename):
        print(f"⚠️ Archivo no encontrado: {filename}")
        return pd.DataFrame()
    try:
        df = pd.read_csv(filename)
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], dayfirst=dayfirst, errors='coerce')
        return df
    except Exception as e:
        print(f"⚠️ Error al cargar {filename}: {e}")
        return pd.DataFrame()

# Archivos de MODO y recargas
df_modo = cargar_csv_con_fechas("modo_diario.csv", "Fecha_Dia")
df_monto = cargar_csv_con_fechas("recargas_monto.csv", "Fecha_Dia")
df_cant = cargar_csv_con_fechas("recargas_cant.csv", "Fecha_Dia")
df_prom = cargar_csv_con_fechas("deposito_promedio.csv", "Fecha_Dia")
df_comp = pd.read_csv("comparativa_modo.csv")
df_mov_modo = cargar_csv_con_fechas("movimientos_modo.csv", "Fecha", dayfirst=True)

# Archivos de análisis general
df_kpis = pd.read_csv("kpis.csv")
df_jugadores = pd.read_csv("jugadores_unicos_por_juego.csv")
df_nuevos = cargar_csv_con_fechas("nuevos_modo.csv", "Fecha_Alta", dayfirst=True)
df_reactivados = cargar_csv_con_fechas("reactivados_modo.csv", "Fecha", dayfirst=True)
df_total_juegos_mes = cargar_csv_con_fechas("total_juegos_mes.csv", "AñoMes", dayfirst=True)
df_total_usuarios_nuevos_modo = pd.read_csv("total_usuarios_nuevos_modo.csv")

# Archivos de bonificaciones
df_kpis_bonif = cargar_csv_con_fechas("kpis_bonificaciones.csv", "Fecha", dayfirst=True)
df_top_bonif = pd.read_csv("top_usuarios_bonificados.csv")

# KPI CARDS
kpi_ids = {
    "Total Nuevos Usuarios desde MODO": "kpi_nuevos_modo",
    "Total apuestas por juego y mes": "grafico_totales_juegos_mes",
    "Promedio depósito $": "kpi_promedio_deposito",
    "Usuarios únicos (cualquier mov.)": "kpi_usuarios_total",
    "Usuarios únicos apostadores": "kpi_usuarios_apostadores",
    "Usuarios únicos que recargaron": "kpi_usuarios_recargaron",
    "Recargas - MODO": "kpi_recargas_modo",
    "Recargas - Retail": "kpi_recargas_retail",
    "Monto MODO $": "kpi_monto_modo",
    "Monto Retail $": "kpi_monto_retail",
    "Usuarios bonificados": "kpi_usuarios_bonificados",
    "Monto total bonificado $": "kpi_monto_bonificado",
}

kpi_labels = pd.concat([df_kpis, df_kpis_bonif])['KPI']
kpi_labels = kpi_labels.dropna().astype(str)

kpi_cards = [
    dbc.Col(dbc.Card([
        dbc.CardHeader(kpi),
        dbc.CardBody(html.H4("0", id=kpi_ids.get(kpi, kpi.replace(' ', '_')), className="card-title"))
    ], color="light")) for kpi in kpi_labels
]

# ================== TABS ==================

tab_main = dbc.Container([
    html.H2("📊 Dashboard General", className="my-4"),
    dbc.Row(kpi_cards, className="mb-4"),
])

tab_tablas = dbc.Container([
    html.H2("📋 Tablas de usuarios únicos", className="my-4"),
    dash_table.DataTable(
        columns=[{"name": c, "id": c} for c in df_jugadores.columns],
        data=df_jugadores.to_dict("records"),
        page_size=10,
        style_table={"overflowX": "auto"},
        style_cell={"textAlign": "left"}
    )
])

bonificaciones_tab = dbc.Container([
    html.H2("🎁 Bonificaciones", className="my-4"),
    dcc.Graph(
        id="grafico_bonificaciones",
        figure=px.bar(
            df_kpis_bonif,
            x="Fecha",
            y="Monto_Total",
            title="Monto total bonificado por día",
            labels={"Monto_Total": "$", "Fecha": "Fecha"}
        )
    ),
    html.H4("Usuarios bonificados por día", className="mt-5"),
    dcc.Graph(
        id="grafico_usuarios_bonificados",
        figure=px.line(
            df_kpis_bonif,
            x="Fecha",
            y="Usuarios_Bonificados",
            title="Cantidad de usuarios bonificados por día",
            labels={"Usuarios_Bonificados": "Usuarios", "Fecha": "Fecha"}
        )
    ),
    html.H4("Top usuarios bonificados", className="mt-5"),
    dash_table.DataTable(
        columns=[{"name": c, "id": c} for c in df_top_bonif.columns],
        data=df_top_bonif.to_dict("records"),
        page_size=10,
        style_table={"overflowX": "auto"},
        style_cell={"textAlign": "left"}
    )
], fluid=True)

# ================== LAYOUT ==================

app.layout = html.Div(
    style={'padding': '150px'},
    children=[
        html.Img(src='/assets/logo.png', style={'display': 'block', 'margin-left': 'auto', 'margin-right': 'auto', 'width': '150px', 'height': '150px'}),
        dbc.Container([
            dcc.Tabs([
                dcc.Tab(label="📊 Dashboard", children=[tab_main]),
                dcc.Tab(label="📋 Tablas de usuarios", children=[tab_tablas]),
                dcc.Tab(label="🎁 Bonificaciones", children=[bonificaciones_tab]),
            ])
        ], fluid=True)
    ]
)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(debug=False, host="0.0.0.0", port=port)
