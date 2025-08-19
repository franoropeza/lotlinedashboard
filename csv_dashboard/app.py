import pandas as pd
import plotly.express as px
from dash import Dash, html, dcc, Input, Output, dash_table
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
#    python generar_reporte_incremental.py
#
# 4. En tu máquina, ejecuta este script para iniciar la aplicación web:
#    python app.py
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
server = app.server

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
            h1, h2, h3, h4, h5, p, label {
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


# Fecha de hito para el filtro de nuevos usuarios y cálculo de recaudación
FECHA_MODO_FULL = pd.Timestamp("2025-07-07")
CSV_DIR = "." # Definir la carpeta de los CSV

# ==================== Funciones de carga de datos ====================
def cargar_csv_con_fechas(filename, date_col, dayfirst=False):
    """
    Carga un archivo CSV y parsea una columna de fecha si existe.
    Retorna un DataFrame vacío si el archivo no existe.
    """
    filepath = os.path.join(CSV_DIR, filename)
    if not os.path.exists(filepath):
        print(f"⚠️ Archivo no encontrado: {filepath}")
        return pd.DataFrame()
    try:
        df = pd.read_csv(filepath)
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], dayfirst=dayfirst, errors='coerce')
        return df
    except Exception as e:
        print(f"⚠️ Error al cargar {filepath}: {e}")
        return pd.DataFrame()

# Cargar datasets
df_modo = cargar_csv_con_fechas("modo_diario.csv", "Fecha_Dia")
df_monto = cargar_csv_con_fechas("recargas_monto.csv", "Fecha_Dia")
df_cant = cargar_csv_con_fechas("recargas_cant.csv", "Fecha_Dia")
df_prom = cargar_csv_con_fechas("deposito_promedio.csv", "Fecha_Dia")
df_comp = pd.read_csv(os.path.join(CSV_DIR, "comparativa_modo.csv")) if os.path.exists(os.path.join(CSV_DIR, "comparativa_modo.csv")) else pd.DataFrame()
df_mov_modo = cargar_csv_con_fechas("movimientos_modo.csv", "Fecha", dayfirst=True)

df_kpis = pd.read_csv(os.path.join(CSV_DIR, "kpis.csv")) if os.path.exists(os.path.join(CSV_DIR, "kpis.csv")) else pd.DataFrame()
df_jugadores = pd.read_csv(os.path.join(CSV_DIR, "jugadores_unicos_por_juego.csv")) if os.path.exists(os.path.join(CSV_DIR, "jugadores_unicos_por_juego.csv")) else pd.DataFrame()
df_nuevos = cargar_csv_con_fechas("nuevos_modo.csv", "Fecha_Alta", dayfirst=True)
df_reactivados = cargar_csv_con_fechas("reactivados_modo.csv", "Fecha", dayfirst=True)
df_total_juegos_mes = cargar_csv_con_fechas("total_juegos_mes.csv", "AñoMes") # AñoMes es un string, no necesita dayfirst
df_total_usuarios_nuevos_modo = pd.read_csv(os.path.join(CSV_DIR, "total_usuarios_nuevos_modo.csv")) if os.path.exists(os.path.join(CSV_DIR, "total_usuarios_nuevos_modo.csv")) else pd.DataFrame({"Valor":[0]})

# Cargar datos para nuevas funcionalidades
df_apuestas = cargar_csv_con_fechas("apuestas_diario.csv", "Fecha_Dia")
df_retencion = pd.read_csv(os.path.join(CSV_DIR, "retencion_cohorts.csv")) if os.path.exists(os.path.join(CSV_DIR, "retencion_cohorts.csv")) else pd.DataFrame()
df_apuestas_full = cargar_csv_con_fechas("apuestas_con_usuarios.csv", "Fecha")

# Llenar DataFrames vacíos para evitar errores de layout
if df_monto.empty:
    print("⚠️ Faltan datos para generar el dashboard principal.")
    df_monto = pd.DataFrame({"Fecha_Dia": [pd.Timestamp.now()], "MODO": [0], "Retail": [0]})
    df_cant = pd.DataFrame({"Fecha_Dia": [pd.Timestamp.now()], "MODO": [0], "Retail": [0]})
    df_modo = pd.DataFrame({"Fecha_Dia": [pd.Timestamp.now()], "Usuarios_Unicos": [0]})
    df_prom = pd.DataFrame({"Fecha_Dia": [pd.Timestamp.now()], "MODO": [0], "Retail": [0]})

if df_apuestas.empty:
    df_apuestas = pd.DataFrame({"Fecha_Dia": [pd.Timestamp.now()], "Recaudacion": [0]})

# Filtros disponibles
fecha_min = df_monto["Fecha_Dia"].min()
fecha_max = df_monto["Fecha_Dia"].max()


# ==================== Layout de la aplicación ====================

# Construir el layout de los KPIs dinámicamente
kpi_cards = []
if not df_kpis.empty:
    for _, row in df_kpis.iterrows():
        kpi_label = row["KPI"]
        kpi_value = row["Valor"]
        
        # Formatear el valor según el KPI
        if "%" in kpi_label:
            formatted_value = f"{kpi_value:,.2f}%"
        elif "$" in kpi_label:
            formatted_value = f"${kpi_value:,.2f}"
        else:
            formatted_value = f"{kpi_value:,.0f}"

        card = dbc.Col(dbc.Card([
            dbc.CardHeader(kpi_label),
            dbc.CardBody(html.H4(formatted_value, className="card-title"))
        ], color="light", className="mb-4"), width=3)
        kpi_cards.append(card)

# Tab: KPIs + Gráficos
tab_main = dbc.Container([
    html.H1("📊 Dashboard de Billeteras", className="text-center my-4"),
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
    html.H2("KPIs Globales", className="mt-4"),
    dbc.Row(kpi_cards),
    
    html.H2("Análisis del Período Seleccionado", className="mt-5"),
    dbc.Row([
        dbc.Col(dbc.Card([
            dbc.CardHeader(html.H5("Recaudación en Período", className="text-center")),
            dbc.CardBody(html.H4("$0", id="kpi_recaudacion_periodo", className="card-title text-center"))
        ], color="warning", outline=True), width=4),
        dbc.Col(dbc.Card([
            dbc.CardHeader(html.H5("Nuevos Usuarios (desde MODO)", className="text-center")),
            dbc.CardBody(html.H4(
                f"{df_total_usuarios_nuevos_modo['Valor'].iloc[0]:,.0f}", 
                className="card-title text-center"
            ))
        ], color="success", outline=True), width=4),
        dbc.Col(dbc.Card([
            dbc.CardHeader(html.H5("Bets Totales por Juego y Mes", className="text-center")),
            dbc.CardBody(dcc.Graph(
                id="grafico_totales_juegos_mes",
                figure=px.bar(
                    df_total_juegos_mes, x="AñoMes", y="Total_Bets", color="Juego",
                    title="Total de Apuestas por Juego y Mes",
                    labels={"Total_Bets": "Total de Apuestas", "AñoMes": "Año-Mes"}
                )
            ))
        ], color="info", outline=True), width=4)
    ], className="mb-4"),

    dbc.Row([dbc.Col(dcc.Graph(id="grafico_monto"))]),
    dbc.Row([dbc.Col(dcc.Graph(id="grafico_cant"))]),
    dbc.Row([dbc.Col(dcc.Graph(id="grafico_prom"))]),
], fluid=True)


# Tab: Tablas interactivas
if not df_nuevos.empty and "Fecha_Alta" in df_nuevos.columns:
    df_nuevos = df_nuevos[df_nuevos["Fecha_Alta"] >= FECHA_MODO_FULL]

tab_tablas = dbc.Container([
    html.H2("📋 Reportes de Usuarios", className="mt-4"),

    html.H4("Jugadores únicos por juego", className="mt-4"),
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

    html.H4("Top 10 usuarios por juego (Dinámico)", className="mt-5"),
    dbc.Row([
        dbc.Col([
            html.Label("Seleccionar Juego:"),
            dcc.Dropdown(
                id='dropdown_juego_top10',
                options=[{'label': j, 'value': j} for j in df_apuestas_full['Juego'].unique()],
                value=df_apuestas_full['Juego'].unique()[0] if not df_apuestas_full.empty else None
            )
        ], width=6)
    ], className="mb-3"),
    dash_table.DataTable(
        id='tabla_top10',
        columns=[],
        data=[],
        page_size=10, style_table={"overflowX": "auto"},
    )
], fluid=True)

# Layout final con tabs
app.layout = html.Div([
    html.Img(src='/assets/logo.png', style={'display': 'block', 'margin-left': 'auto', 'margin-right': 'auto', 'width': '150px', 'height': '150px', 'margin-top':'20px'}),
    dbc.Container([
        dcc.Tabs([
            dcc.Tab(label="📊 Dashboard", children=[tab_main]),
            dcc.Tab(label="📋 Tablas de usuarios", children=[tab_tablas]),
            dcc.Tab(label="📈 Retención de Usuarios", children=[
                dbc.Container([
                    html.H2("Análisis de Retención de Nuevos Usuarios", className="mt-4"),
                    html.P("Este gráfico muestra el % de usuarios nuevos de cada mes que realizaron su primera apuesta dentro de los 7 y 30 días de su primer movimiento."),
                    dcc.Graph(
                        figure=px.bar(
                            df_retencion,
                            x="Cohorte_Mes",
                            y=["Tasa_Retencion_7_Dias", "Tasa_Retencion_30_Dias"],
                            title="Tasa de Retención de Nuevos Usuarios por Cohorte Mensual",
                            labels={"Cohorte_Mes": "Mes de Adquisición", "value": "Tasa de Retención (%)"},
                            barmode='group'
                        )
                    )
                ], fluid=True)
            ]),
        ])
    ], fluid=True, style={'padding': '20px'})
])

# ==================== Callbacks ====================
@app.callback(
    Output("kpi_recaudacion_periodo", "children"),
    Output("grafico_monto", "figure"),
    Output("grafico_cant", "figure"),
    Output("grafico_prom", "figure"),
    Input("filtro_fecha", "start_date"),
    Input("filtro_fecha", "end_date")
)
def actualizar_dashboard(start, end):
    start_dt = pd.to_datetime(start)
    end_dt = pd.to_datetime(end)

    # Filtrar dataframes por rango de fecha
    df_monto_filtrado = df_monto[(df_monto["Fecha_Dia"] >= start_dt) & (df_monto["Fecha_Dia"] <= end_dt)]
    df_cant_filtrado = df_cant[(df_cant["Fecha_Dia"] >= start_dt) & (df_cant["Fecha_Dia"] <= end_dt)]
    df_prom_filtrado = df_prom[(df_prom["Fecha_Dia"] >= start_dt) & (df_prom["Fecha_Dia"] <= end_dt)]
    df_apuestas_filtrado = df_apuestas[(df_apuestas["Fecha_Dia"] >= start_dt) & (df_apuestas["Fecha_Dia"] <= end_dt)]
    
    # Calcular recaudación para el período seleccionado
    recaudacion_periodo = df_apuestas_filtrado["Recaudacion"].sum() if not df_apuestas_filtrado.empty else 0

    # Generar figuras de gráficos
    fig_monto = px.line(df_monto_filtrado, x="Fecha_Dia", y=["MODO", "Retail"],
                        title="$ por día por canal", labels={"value": "$", "variable": "Canal"})
    fig_cant = px.bar(df_cant_filtrado, x="Fecha_Dia", y=["MODO", "Retail"],
                      title="Recargas por día por canal", labels={"value": "Cantidad", "variable": "Canal"})
    fig_prom = px.line(df_prom_filtrado, x="Fecha_Dia", y=["MODO", "Retail"],
                       title="Depósito promedio diario – MODO vs Retail", labels={"value": "$", "variable": "Canal"})
    
    return (
        f"${recaudacion_periodo:,.2f}",
        fig_monto,
        fig_cant,
        fig_prom,
    )

# Callback para la tabla dinámica Top 10
@app.callback(
    Output("tabla_top10", "columns"),
    Output("tabla_top10", "data"),
    Input("filtro_fecha", "start_date"),
    Input("filtro_fecha", "end_date"),
    Input("dropdown_juego_top10", "value")
)
def actualizar_top10(start, end, juego_seleccionado):
    if df_apuestas_full.empty or not juego_seleccionado:
        return [], []

    start_dt = pd.to_datetime(start)
    end_dt = pd.to_datetime(end)
    
    # 1. Filtrar por fecha y juego
    mask = (
        (df_apuestas_full["Fecha"] >= start_dt) &
        (df_apuestas_full["Fecha"] <= end_dt) &
        (df_apuestas_full["Juego"] == juego_seleccionado)
    )
    df_filtrado = df_apuestas_full.loc[mask]

    if df_filtrado.empty:
        return [], []

    # 2. Agrupar y agregar para obtener los totales
    top_users = df_filtrado.groupby(["Documento", "Usuario", "Correo"]).agg(
        Total_Gastado=("Importe", "sum"),
        Total_Apuestas=("Importe", "count")
    ).reset_index()

    # 3. Ordenar y tomar el top 10
    top_users = top_users.sort_values("Total_Gastado", ascending=False).head(10)
    
    # Formatear columnas para la tabla
    top_users["Total_Gastado"] = top_users["Total_Gastado"].map('${:,.2f}'.format)
    
    columns = [{"name": c, "id": c} for c in top_users.columns]
    data = top_users.to_dict("records")
    
    return columns, data


if __name__ == "__main__":
    # Render provides the port to use in an environment variable.
    # The host must be '0.0.0.0' to be accessible from outside the container.
    # Debug mode should be False in production.
    port = int(os.environ.get("PORT", 10000))
    app.run(debug=False, host="0.0.0.0", port=port)