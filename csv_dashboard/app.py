import pandas as pd
import plotly.express as px
from dash import Dash, html, dcc, Input, Output
import dash_bootstrap_components as dbc
import os

# =========================================================================
# === INSTRUCCIONES PARA COMPARTIR EL DASHBOARD CON EL EQUIPO =============
# =========================================================================
# Para compartir este dashboard con tu equipo, sigue estos pasos:
#
# 1. Asegúrate de tener todos los archivos necesarios en una carpeta:
#    - Este archivo: `app.py`
#    - El script para generar los datos: `generar_reporte_incremental.py`
#    - La carpeta con los datos de origen: `data/`
#
# 2. Tu equipo debe instalar las librerías necesarias de Python:
#    - pandas
#    - plotly
#    - dash
#    - dash-bootstrap-components
#    Esto se puede hacer con el siguiente comando en la terminal:
#    pip install pandas plotly dash dash-bootstrap-components
#
# 3. En tu máquina, ejecuta el script de generación de reportes para
#    asegurarte de que los archivos CSV estén actualizados:
#    python3.13 generar_reporte_incremental.py
#
# 4. En tu máquina, ejecuta este script para iniciar la aplicación web:
#    python3.13 app.py
#
# 5. La aplicación se iniciará en un servidor web local, generalmente en
#    http://127.0.0.1:8050. Comparte esta URL con tu equipo.
#
# 6. Para que el equipo pueda acceder a la aplicación, tu máquina debe estar
#    encendida y la aplicación en ejecución. Si necesitan un acceso más
#    permanente, la aplicación se puede desplegar en un servidor web
#    dedicado (como Heroku, PythonAnywhere, etc.), pero esto requiere
#    una configuración adicional.
# =========================================================================


# Inicializar app
app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.title = "Dashboard Lotería de Salta"

# Estilos CSS personalizados para el fondo y padding
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
            h1, h2, h3, h4, h5, h6 {
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


# Fecha de hito para el filtro de nuevos usuarios
FECHA_MODO_FULL = pd.Timestamp("2025-07-07")

# ==================== Funciones de carga de datos ====================
def cargar_csv_con_fechas(filename, date_col, dayfirst=False):
    """
    Carga un archivo CSV y parsea una columna de fecha si existe.
    Retorna un DataFrame vacío si el archivo no existe.
    """
    if not os.path.exists(filename):
        print(f"⚠️ Archivo no encontrado: {filename}")
        return pd.DataFrame()
    try:
        # Intentar leer el CSV con la columna de fecha
        df = pd.read_csv(filename)
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], dayfirst=dayfirst, errors='coerce')
        return df
    except Exception as e:
        print(f"⚠️ Error al cargar {filename}: {e}")
        return pd.DataFrame()

# Cargar datasets
# Archivos con fechas (formato YYYY-MM-DD, no usar dayfirst=True)
df_modo = cargar_csv_con_fechas("modo_diario.csv", "Fecha_Dia")
df_monto = cargar_csv_con_fechas("recargas_monto.csv", "Fecha_Dia")
df_cant = cargar_csv_con_fechas("recargas_cant.csv", "Fecha_Dia")
df_prom = cargar_csv_con_fechas("deposito_promedio.csv", "Fecha_Dia")
df_comp = pd.read_csv("comparativa_modo.csv")
df_mov_modo = cargar_csv_con_fechas("movimientos_modo.csv", "Fecha", dayfirst=True)

df_kpis = pd.read_csv("kpis.csv")
df_jugadores = pd.read_csv("jugadores_unicos_por_juego.csv")
df_nuevos = cargar_csv_con_fechas("nuevos_modo.csv", "Fecha_Alta", dayfirst=True)
df_reactivados = cargar_csv_con_fechas("reactivados_modo.csv", "Fecha", dayfirst=True)
df_total_juegos_mes = cargar_csv_con_fechas("total_juegos_mes.csv", "AñoMes", dayfirst=True)
df_total_usuarios_nuevos_modo = pd.read_csv("total_usuarios_nuevos_modo.csv")


# Llenar DataFrames vacíos para evitar errores de layout
if df_modo.empty or df_monto.empty:
    print("⚠️ Faltan datos para generar el dashboard principal.")
    df_monto = pd.DataFrame({"Fecha_Dia": [pd.Timestamp.now()], "MODO": [0], "Retail": [0]})
    df_cant = pd.DataFrame({"Fecha_Dia": [pd.Timestamp.now()], "MODO": [0], "Retail": [0]})
    df_modo = pd.DataFrame({"Fecha_Dia": [pd.Timestamp.now()], "Usuarios_Unicos": [0]})
    df_prom = pd.DataFrame({"Fecha_Dia": [pd.Timestamp.now()], "MODO": [0], "Retail": [0]})

# Filtros disponibles
fecha_min = df_monto["Fecha_Dia"].min()
fecha_max = df_monto["Fecha_Dia"].max()

# Layout
from dash import dash_table

# Definir los IDs para los KPIs
kpi_ids = {
    "Total Nuevos Usuarios desde MODO": "kpi_nuevos_modo",
    "Total Bets por juego y mes": "grafico_totales_juegos_mes",
    "Promedio depósito $": "kpi_promedio_deposito",
    "Usuarios únicos (cualquier mov.)": "kpi_usuarios_total",
    "Usuarios únicos apostadores": "kpi_usuarios_apostadores",
    "Usuarios únicos que recargaron": "kpi_usuarios_recargaron",
    "Recargas - MODO": "kpi_recargas_modo",
    "Recargas - Retail": "kpi_recargas_retail",
    "Monto MODO $": "kpi_monto_modo",
    "Monto Retail $": "kpi_monto_retail",
}

# Construir el layout de los KPIs dinámicamente
kpi_cards = [
    dbc.Col(dbc.Card([
        dbc.CardHeader(kpi),
        dbc.CardBody(html.H4(f"0", id=kpi_ids.get(kpi, kpi.replace(' ', '_')), className="card-title"))
    ], color="light")) for kpi in df_kpis["KPI"]
]

# Tab: KPIs + Gráficos
tab_main = dbc.Container([
    html.H1("📊 Dashboard Lotería de Salta", className="text-center my-4"),
    dbc.Row([
        dbc.Col([
            dcc.DatePickerRange(
                id="filtro_fecha",
                start_date=fecha_min,
                end_date=fecha_max,
                min_date_allowed=fecha_min,
                max_date_allowed=fecha_max,
                display_format="YYYY-MM-DD",
                className="mb-4"
            )
        ])
    ]),
    dbc.Row(kpi_cards, className="mb-4"),
    
    # Nuevos KPIs y gráficos
    html.H2("Resumen de Usuarios", className="mt-5"),
    dbc.Row([
        dbc.Col(dbc.Card([
            dbc.CardHeader(html.H5("Nuevos Usuarios desde MODO", className="text-center")),
            dbc.CardBody(html.H4(
                f"{df_total_usuarios_nuevos_modo['Valor'].iloc[0]:,.0f}", 
                id="kpi_nuevos_modo", className="card-title text-center"
            ))
        ], color="success", outline=True), width=6),
        dbc.Col(dbc.Card([
            dbc.CardHeader(html.H5("Total Bets por juego y mes", className="text-center")),
            dbc.CardBody(dcc.Graph(
                id="grafico_totales_juegos_mes",
                figure=px.bar(
                    df_total_juegos_mes,
                    x="AñoMes",
                    y="Total_Bets",
                    color="Juego",
                    title="Total de Apuestas por Juego y Mes",
                    labels={"Total_Bets": "Total de Apuestas", "AñoMes": "Año-Mes"}
                )
            ))
        ], color="info", outline=True), width=6)
    ], className="mb-4"),


    dbc.Row([dbc.Col(dcc.Graph(id="grafico_monto"))]),
    dbc.Row([dbc.Col(dcc.Graph(id="grafico_cant"))]),
    dbc.Row([dbc.Col(dcc.Graph(id="grafico_modo"))]),
    dbc.Row([dbc.Col(dcc.Graph(id="grafico_prom"))]),
    dbc.Row([dbc.Col(dcc.Graph(
        id="grafico_comp",
        figure=px.bar(df_comp, x="Periodo", y=["Depositos_$", "Recaudacion_$"],
                      barmode="group", title="Antes/Después MODO",
                      labels={"value": "$", "variable": "Tipo"})
    ))])
], fluid=True)

# Tab: Tablas interactivas
# Cargar top 10 por juego automáticamente
top10_files = [f for f in os.listdir() if f.startswith("top10_") and f.endswith(".csv")]
top10_tabs = []

for fname in top10_files:
    juego = fname.replace("top10_", "").replace(".csv", "").replace("_", " ").title()
    df_top = pd.read_csv(f"{fname}")
    table = dash_table.DataTable(
        columns=[{"name": c, "id": c} for c in df_top.columns],
        data=df_top.to_dict("records"),
        page_size=10,
        style_table={"overflowX": "auto"},
        style_cell={"textAlign": "left"}
    )
    top10_tabs.append(dcc.Tab(label=f"Top 10 – {juego}", children=[table]))

# Filtrar df_nuevos por la fecha del hito
if not df_nuevos.empty and "Fecha_Alta" in df_nuevos.columns:
    df_nuevos = df_nuevos[df_nuevos["Fecha_Alta"] >= FECHA_MODO_FULL]

tab_tablas = dbc.Container([
    html.H2("📋 Reportes de usuarios", className="mt-4"),

    html.H4("Jugadores únicos por juego"),
    dash_table.DataTable(
        columns=[{"name": c, "id": c} for c in df_jugadores.columns],
        data=df_jugadores.to_dict("records"),
        page_size=10, style_table={"overflowX": "auto"},
    ),

    html.H4(f"Nuevos usuarios registrados a partir del {FECHA_MODO_FULL.strftime('%d/%m/%Y')}", className="mt-5"),
    dash_table.DataTable(
        columns=[{"name": c, "id": c} for c in df_nuevos.columns],
        data=df_nuevos.to_dict("records"),
        page_size=10, style_table={"overflowX": "auto"},
    ),

    html.H4("Usuarios reactivados", className="mt-5"),
    dash_table.DataTable(
        columns=[{"name": c, "id": c} for c in df_reactivados.columns],
        data=df_reactivados.to_dict("records"),
        page_size=10, style_table={"overflowX": "auto"},
    ),

    html.H4("Top 10 usuarios por juego", className="mt-5"),
    dcc.Tabs(top10_tabs)
], fluid=True)

# Layout final con tabs
app.layout = html.Div(
    style={'padding': '150px'},
    children=[
        html.Img(src='/assets/logo.png', style={'display': 'block', 'margin-left': 'auto', 'margin-right': 'auto', 'width': '150px', 'height': '150px'}),
        dbc.Container([
            dcc.Tabs([
                dcc.Tab(label="📊 Dashboard", children=[tab_main]),
                dcc.Tab(label="📋 Tablas de usuarios", children=[tab_tablas]),
            ])
        ], fluid=True)
    ]
)

# Callback para actualizar todos los gráficos y KPIs
@app.callback(
    Output("kpi_promedio_deposito", "children"),
    Output("kpi_usuarios_total", "children"),
    Output("kpi_usuarios_apostadores", "children"),
    Output("kpi_usuarios_recargaron", "children"),
    Output("kpi_recargas_modo", "children"),
    Output("kpi_recargas_retail", "children"),
    Output("kpi_monto_modo", "children"),
    Output("kpi_monto_retail", "children"),
    Output("grafico_monto", "figure"),
    Output("grafico_cant", "figure"),
    Output("grafico_modo", "figure"),
    Output("grafico_prom", "figure"),
    Output("grafico_totales_juegos_mes", "figure"),
    Output("grafico_comp", "figure"),
    Input("filtro_fecha", "start_date"),
    Input("filtro_fecha", "end_date")
)
def actualizar_dashboard(start, end):
    # Filtrar dataframes por rango de fecha
    df_monto_filtrado = df_monto[(df_monto["Fecha_Dia"] >= start) & (df_monto["Fecha_Dia"] <= end)]
    df_cant_filtrado = df_cant[(df_cant["Fecha_Dia"] >= start) & (df_cant["Fecha_Dia"] <= end)]
    df_modo_filtrado = df_modo[(df_modo["Fecha_Dia"] >= start) & (df_modo["Fecha_Dia"] <= end)]
    df_prom_filtrado = df_prom[(df_prom["Fecha_Dia"] >= start) & (df_prom["Fecha_Dia"] <= end)]
    df_total_juegos_mes_filtrado = df_total_juegos_mes[(df_total_juegos_mes["AñoMes"] >= start[:7]) & (df_total_juegos_mes["AñoMes"] <= end[:7])]
    
    # Actualizar KPIs
    promedio_deposito = df_prom_filtrado["MODO"].mean() if not df_prom_filtrado.empty else 0
    usuarios_total = df_cant_filtrado["MODO"].sum() + df_cant_filtrado["Retail"].sum() if not df_cant_filtrado.empty else 0
    usuarios_apostadores = df_modo_filtrado["Usuarios_Unicos"].sum() if not df_modo_filtrado.empty else 0
    usuarios_recargaron = df_cant_filtrado["MODO"].sum() + df_cant_filtrado["Retail"].sum() if not df_cant_filtrado.empty else 0
    recargas_modo = df_cant_filtrado["MODO"].sum() if not df_cant_filtrado.empty else 0
    recargas_retail = df_cant_filtrado["Retail"].sum() if not df_cant_filtrado.empty else 0
    monto_modo = df_monto_filtrado["MODO"].sum() if not df_monto_filtrado.empty else 0
    monto_retail = df_monto_filtrado["Retail"].sum() if not df_monto_filtrado.empty else 0

    # Generar figuras de gráficos
    fig_monto = px.line(df_monto_filtrado, x="Fecha_Dia", y=["MODO", "Retail"],
                        title="$ por día por canal", labels={"value": "$", "variable": "Canal"})
    fig_cant = px.bar(df_cant_filtrado, x="Fecha_Dia", y=["MODO", "Retail"],
                      title="Recargas por día por canal", labels={"value": "Cantidad", "variable": "Canal"})
    fig_modo = px.line(df_modo_filtrado, x="Fecha_Dia", y="Usuarios_Unicos",
                       title="Usuarios únicos MODO por día")
    fig_prom = px.line(df_prom_filtrado, x="Fecha_Dia", y=["MODO", "Retail"],
                       title="Depósito promedio diario — MODO vs Retail", labels={"value": "$", "variable": "Canal"})
    fig_total_juegos_mes = px.bar(
        df_total_juegos_mes_filtrado,
        x="AñoMes",
        y="Total_Bets",
        color="Juego",
        title="Total de Apuestas por Juego y Mes",
        labels={"Total_Bets": "Total de Apuestas", "AñoMes": "Año-Mes"}
    )
    fig_comp = px.bar(df_comp, x="Periodo", y=["Depositos_$", "Recaudacion_$"],
                      barmode="group", title="Antes/Después MODO",
                      labels={"value": "$", "variable": "Tipo"})
    
    # Devolver los valores actualizados
    return (
        f"{promedio_deposito:,.2f}",
        f"{usuarios_total:,.0f}",
        f"{usuarios_apostadores:,.0f}",
        f"{usuarios_recargaron:,.0f}",
        f"{recargas_modo:,.0f}",
        f"{recargas_retail:,.0f}",
        f"{monto_modo:,.2f}",
        f"{monto_retail:,.2f}",
        fig_monto,
        fig_cant,
        fig_modo,
        fig_prom,
        fig_total_juegos_mes,
        fig_comp
    )


if __name__ == '__main__':
    app.run(debug=False)