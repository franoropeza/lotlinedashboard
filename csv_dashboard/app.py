import pandas as pd
import plotly.express as px
from dash import Dash, html, dcc, Input, Output, dash_table
import dash_bootstrap_components as dbc
import os
import numpy as np

# =========================================================================
# === INSTRUCCIONES PARA DESPLIEGUE =======================================
# =========================================================================
# 1.  Asegúrate de que tu script `generar_reporte_incremental.py` esté
#     actualizado para crear todos los CSVs necesarios en la carpeta
#     `csv_dashboard/`.
#
# 2.  Ejecuta `generar_reporte_incremental.py` localmente para crear
#     los archivos de datos.
#
# 3.  Sube la carpeta `csv_dashboard/` junto con `app.py` a tu
#     repositorio de Git para que Render pueda acceder a los datos.
#
#     NOTA: Para una solución más robusta, los datos deberían estar
#     en una base de datos externa, no en archivos CSV en el repositorio.
# =========================================================================

# Inicializar app
app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.title = "Dashboard de Billeteras"
server = app.server

# Estilos CSS personalizados
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

# Configuración de archivos
FECHA_MODO_FULL = pd.Timestamp("2025-07-07")
CSV_DIR = "."

# ==================== Funciones de carga de datos ====================
def cargar_csv_con_fechas(filename, date_col, dayfirst=False):
    """Carga un archivo CSV y convierte una columna de fecha si existe."""
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
df_monto = cargar_csv_con_fechas("recargas_monto.csv", "Fecha_Dia")
df_cant = cargar_csv_con_fechas("recargas_cant.csv", "Fecha_Dia")
df_prom = cargar_csv_con_fechas("deposito_promedio.csv", "Fecha_Dia")
df_jugadores = pd.read_csv(os.path.join(CSV_DIR, "jugadores_unicos_por_juego.csv")) if os.path.exists(os.path.join(CSV_DIR, "jugadores_unicos_por_juego.csv")) else pd.DataFrame()
df_nuevos = cargar_csv_con_fechas("nuevos_modo.csv", "Fecha_Alta", dayfirst=True)
df_reactivados = cargar_csv_con_fechas("reactivados_modo.csv", "Fecha", dayfirst=True)
df_total_juegos_mes = cargar_csv_con_fechas("total_juegos_mes.csv", "AñoMes")


# Cargar datos para nuevas funcionalidades
df_apuestas = cargar_csv_con_fechas("apuestas_diario.csv", "Fecha_Dia")
df_retencion = pd.read_csv(os.path.join(CSV_DIR, "retencion_cohorts.csv")) if os.path.exists(os.path.join(CSV_DIR, "retencion_cohorts.csv")) else pd.DataFrame()
df_apuestas_full = cargar_csv_con_fechas("apuestas_con_usuarios.csv", "Fecha")

# Llenar DataFrames vacíos para evitar errores de layout si faltan archivos
if df_monto.empty:
    print("⚠️ Faltan datos para generar el dashboard principal.")
    df_monto = pd.DataFrame({"Fecha_Dia": [pd.Timestamp.now()], "MODO": [0], "Retail": [0]})
    df_cant = pd.DataFrame({"Fecha_Dia": [pd.Timestamp.now()], "MODO": [0], "Retail": [0]})
    df_prom = pd.DataFrame({"Fecha_Dia": [pd.Timestamp.now()], "MODO": [0], "Retail": [0]})

if df_apuestas.empty:
    df_apuestas = pd.DataFrame({"Fecha_Dia": [pd.Timestamp.now()], "Recaudacion": [0]})

# Filtros disponibles
fecha_min = df_monto["Fecha_Dia"].min()
fecha_max = df_monto["Fecha_Dia"].max()

# === Bonificaciones (nuevos CSVs) ===
df_bonos_kpis   = cargar_csv_con_fechas("kpis_bonificaciones.csv", "Fecha_Dia", dayfirst=True)
df_bonos_diario = cargar_csv_con_fechas("bonos_diario.csv", "Fecha_Dia", dayfirst=True)
df_bonos_detalle = cargar_csv_con_fechas("bonos_resumen.csv", "Fecha_Bono", dayfirst=True)

# Cols mínimas esperadas; si faltan, crear dataframes vacíos compatibles
for name, df, cols in [
    ("df_bonos_kpis", df_bonos_kpis, ["Fecha_Dia","Bonos_Otorgados","Monto_Bonos","Jugaron_PostBono","Agotaron_Bono","Recargaron_Luego","Registro_Cant","Deposito_Cant"]),
    ("df_bonos_diario", df_bonos_diario, ["Fecha_Dia","Bonos_Otorgados","Monto_Bonos","Registro_Cant","Deposito_Cant"]),
]:
    if df.empty:
        locals()[name] = pd.DataFrame([{c: (pd.Timestamp.now().date() if "Fecha" in c else 0)} for c in cols]).T.reset_index()
        locals()[name].columns = cols

# ==================== Layout de la aplicación ====================

# Tab: KPIs + Gráficos
tab_main = dbc.Container([
    html.H1("📊 Dashboard de Billeteras", className="text-center my-4"),
    
    # --- FILTRO DE FECHA ---
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

    # --- KPIs DINÁMICOS (se actualizan con la fecha) ---
    html.H2("Análisis del Período Seleccionado", className="mt-4"),
    dbc.Row([
        # Fila 1 de KPIs dinámicos
        dbc.Col(dbc.Card([
            dbc.CardHeader("Promedio depósito $"),
            dbc.CardBody(html.H4(id="kpi_promedio_deposito", className="card-title"))
        ], color="light", className="mb-4"), width=3),
        dbc.Col(dbc.Card([
            dbc.CardHeader("Usuarios únicos apostadores"),
            dbc.CardBody(html.H4(id="kpi_usuarios_apostadores", className="card-title"))
        ], color="light", className="mb-4"), width=3),
        dbc.Col(dbc.Card([
            dbc.CardHeader("Usuarios únicos que recargaron"),
            dbc.CardBody(html.H4(id="kpi_usuarios_recargaron", className="card-title"))
        ], color="light", className="mb-4"), width=3),
         dbc.Col(dbc.Card([
            dbc.CardHeader("Recaudación en Período"),
            dbc.CardBody(html.H4(id="kpi_recaudacion_periodo", className="card-title"))
        ], color="warning", outline=True, className="mb-4"), width=3),
    ]),
    dbc.Row([
        # Fila 2 de KPIs dinámicos
        dbc.Col(dbc.Card([
            dbc.CardHeader("Recargas - MODO"),
            dbc.CardBody(html.H4(id="kpi_recargas_modo", className="card-title"))
        ], color="light", className="mb-4"), width=3),
        dbc.Col(dbc.Card([
            dbc.CardHeader("Recargas - Retail"),
            dbc.CardBody(html.H4(id="kpi_recargas_retail", className="card-title"))
        ], color="light", className="mb-4"), width=3),
        dbc.Col(dbc.Card([
            dbc.CardHeader("Monto MODO $"),
            dbc.CardBody(html.H4(id="kpi_monto_modo", className="card-title"))
        ], color="light", className="mb-4"), width=3),
        dbc.Col(dbc.Card([
            dbc.CardHeader("Monto Retail $"),
            dbc.CardBody(html.H4(id="kpi_monto_retail", className="card-title"))
        ], color="light", className="mb-4"), width=3),
    ]),

    # --- GRÁFICOS DINÁMICOS ---
    dbc.Row([dbc.Col(dcc.Graph(id="grafico_evolucion_juegos"))]),
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
    html.Img(src='assets/logo.png', style={'display': 'block', 'margin-left': 'auto', 'margin-right': 'auto', 'width': '150px', 'height': '150px', 'margin-top':'20px'}),
    dbc.Container([
        dcc.Tabs([
            dcc.Tab(label="📊 Dashboard", children=[tab_main]),
            dcc.Tab(label="📋 Tablas de usuarios", children=[tab_tablas]),
            dcc.Tab(label="🎁 Datos de Bonificaciones", children=[
    dbc.Container([
        html.H2("Datos de Bonificaciones", className="mt-4"),
        html.P("Evolución diaria de bonos y comportamiento posterior de los usuarios bonificados."),

        # KPIs
        dbc.Row([
            dbc.Col(dbc.Card([dbc.CardHeader("Bonos otorgados"), dbc.CardBody(html.H4(id="kpi_bonos_otorgados"))], color="light"), md=2),
            dbc.Col(dbc.Card([dbc.CardHeader("Monto en bonos $"), dbc.CardBody(html.H4(id="kpi_monto_bonos"))], color="light"), md=2),
            dbc.Col(dbc.Card([dbc.CardHeader("% que jugaron"), dbc.CardBody(html.H4(id="kpi_pct_jugaron"))], color="light"), md=2),
            dbc.Col(dbc.Card([dbc.CardHeader("% que agotaron"), dbc.CardBody(html.H4(id="kpi_pct_agotaron"))], color="light"), md=2),
            dbc.Col(dbc.Card([dbc.CardHeader("% que recargaron"), dbc.CardBody(html.H4(id="kpi_pct_recargaron"))], color="light"), md=2),
        ], className="my-3"),

        # Gráficos
        dbc.Row([dbc.Col(dcc.Graph(id="graf_bonos_evolucion"))]),
        dbc.Row([dbc.Col(dcc.Graph(id="graf_bonos_tipo"))]),
        dbc.Row([dbc.Col(dcc.Graph(id="graf_bonos_funnel"))]),

        # Detalle (tabla)
        html.H4("Detalle de bonificaciones (filtrado por fecha)"),
        dash_table.DataTable(
            id="tabla_bonos_detalle",
            columns=[{"name": c, "id": c} for c in (df_bonos_detalle.columns if not df_bonos_detalle.empty else ["DNI_NORM","Email","Fecha_Bono","Monto_Bono","Tipo_Bono","Hizo_Jugada_PostBono","Fecha_Primera_Jugada","Fecha_Agote_Bono","Recargo_despues_Agote"])],
            data=[],
            page_size=10,
            style_table={"overflowX":"auto"}
        )
    ], fluid=True)
]),

        ])
    ], fluid=True, style={'padding': '20px'})
])

# ==================== Callbacks ====================
@app.callback(
    # --- Salidas para KPIs Dinámicos ---
    Output("kpi_promedio_deposito", "children"),
    Output("kpi_usuarios_apostadores", "children"),
    Output("kpi_usuarios_recargaron", "children"),
    Output("kpi_recaudacion_periodo", "children"),
    Output("kpi_recargas_modo", "children"),
    Output("kpi_recargas_retail", "children"),
    Output("kpi_monto_modo", "children"),
    Output("kpi_monto_retail", "children"),
    # --- Salidas para Gráficos ---
    Output("grafico_evolucion_juegos", "figure"),
    Output("grafico_monto", "figure"),
    Output("grafico_cant", "figure"),
    Output("grafico_prom", "figure"),
    # --- Entradas ---
    Input("filtro_fecha", "start_date"),
    Input("filtro_fecha", "end_date")
)
def actualizar_dashboard(start, end):
    start_dt = pd.to_datetime(start)
    end_dt = pd.to_datetime(end)

    # Filtrar todos los dataframes necesarios por el rango de fecha
    df_monto_filtrado = df_monto[(df_monto["Fecha_Dia"] >= start_dt) & (df_monto["Fecha_Dia"] <= end_dt)]
    df_cant_filtrado = df_cant[(df_cant["Fecha_Dia"] >= start_dt) & (df_cant["Fecha_Dia"] <= end_dt)]
    df_prom_filtrado = df_prom[(df_prom["Fecha_Dia"] >= start_dt) & (df_prom["Fecha_Dia"] <= end_dt)]
    df_apuestas_filtrado = df_apuestas[(df_apuestas["Fecha_Dia"] >= start_dt) & (df_apuestas["Fecha_Dia"] <= end_dt)]
    df_apostadores_filtrado = df_apuestas_full[(df_apuestas_full["Fecha"] >= start_dt) & (df_apuestas_full["Fecha"] <= end_dt)]

    # --- Calcular los valores para los KPIs dinámicos ---
    promedio_deposito_modo = np.nanmean(df_prom_filtrado["MODO"]) if "MODO" in df_prom_filtrado and not df_prom_filtrado["MODO"].empty else 0
    promedio_deposito_retail = np.nanmean(df_prom_filtrado["Retail"]) if "Retail" in df_prom_filtrado and not df_prom_filtrado["Retail"].empty else 0
    total_depositos = df_cant_filtrado['MODO'].sum() + df_cant_filtrado['Retail'].sum()
    total_monto = df_monto_filtrado['MODO'].sum() + df_monto_filtrado['Retail'].sum()
    promedio_total = total_monto / total_depositos if total_depositos > 0 else 0

    usuarios_apostadores = df_apostadores_filtrado["Documento"].nunique()
    recargas_modo_total = df_cant_filtrado["MODO"].sum()
    recargas_retail_total = df_cant_filtrado["Retail"].sum()
    
    # Un cálculo más preciso para usuarios únicos que recargaron requeriría un log detallado de cargas.
    # Aquí sumamos las recargas de ambos canales como proxy.
    usuarios_recargaron = recargas_modo_total + recargas_retail_total

    recaudacion_periodo = df_apuestas_filtrado["Recaudacion"].sum()
    monto_modo = df_monto_filtrado["MODO"].sum()
    monto_retail = df_monto_filtrado["Retail"].sum()

    # --- Generar figuras de gráficos ---
    fig_monto = px.line(df_monto_filtrado, x="Fecha_Dia", y=["MODO", "Retail"], title="$ por día por canal", labels={"value": "$", "variable": "Canal"})
    fig_cant = px.bar(df_cant_filtrado, x="Fecha_Dia", y=["MODO", "Retail"], title="Recargas por día por canal", labels={"value": "Cantidad", "variable": "Canal"})
    fig_prom = px.line(df_prom_filtrado, x="Fecha_Dia", y=["MODO", "Retail"], title="Depósito promedio diario", labels={"value": "$", "variable": "Canal"})
    
    # --- Lógica para nuevo gráfico de evolución de juegos ---
    start_month = pd.to_datetime(start).strftime('%Y-%m')
    end_month = pd.to_datetime(end).strftime('%Y-%m')
    df_juegos_filtrado = df_total_juegos_mes[
        (df_total_juegos_mes["AñoMes"] >= start_month) &
        (df_total_juegos_mes["AñoMes"] <= end_month)
    ]
    fig_evolucion_juegos = px.bar(
        df_juegos_filtrado,
        x="AñoMes",
        y="Total_Bets",
        color="Juego",
        title="Evolución Mensual de Apuestas por Juego",
        labels={"AñoMes": "Mes", "Total_Bets": "Cantidad de Apuestas"}
    )

    return (
        f"${promedio_total:,.2f}",
        f"{usuarios_apostadores:,.0f}",
        f"{int(usuarios_recargaron):,.0f}",
        f"${recaudacion_periodo:,.2f}",
        f"{recargas_modo_total:,.0f}",
        f"{recargas_retail_total:,.0f}",
        f"${monto_modo:,.2f}",
        f"${monto_retail:,.2f}",
        fig_evolucion_juegos,
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
    
    mask = (
        (df_apuestas_full["Fecha"] >= start_dt) &
        (df_apuestas_full["Fecha"] <= end_dt) &
        (df_apuestas_full["Juego"] == juego_seleccionado)
    )
    df_filtrado = df_apuestas_full.loc[mask]

    if df_filtrado.empty:
        return [], []

    top_users = df_filtrado.groupby(["Documento", "Usuario", "Correo"]).agg(
        Total_Gastado=("Importe", "sum"),
        Total_Apuestas=("Importe", "count")
    ).reset_index()

    top_users = top_users.sort_values("Total_Gastado", ascending=False).head(10)
    top_users["Total_Gastado"] = top_users["Total_Gastado"].map('${:,.2f}'.format)
    
    columns = [{"name": c, "id": c} for c in top_users.columns]
    data = top_users.to_dict("records")
    
    return columns, data
from dash.dependencies import Input, Output
import plotly.express as px

@app.callback(
    Output("kpi_bonos_otorgados", "children"),
    Output("kpi_monto_bonos", "children"),
    Output("kpi_pct_jugaron", "children"),
    Output("kpi_pct_agotaron", "children"),
    Output("kpi_pct_recargaron", "children"),
    Output("graf_bonos_evolucion", "figure"),
    Output("graf_bonos_tipo", "figure"),
    Output("graf_bonos_funnel", "figure"),
    Output("tabla_bonos_detalle", "data"),
    Input("filtro_fecha", "start_date"),
    Input("filtro_fecha", "end_date"),
)
def actualizar_tab_bonos(start, end):
    if start is None or end is None:
        return ["0","$0","0%","0%","0%", px.line(), px.bar(), px.bar(), []]

    # ► Comparar siempre Timestamp vs Timestamp (nada de .date())
    s = pd.to_datetime(start, errors="coerce").normalize()
    e = pd.to_datetime(end, errors="coerce").normalize()

    # Asegurar que las columnas de fecha sean datetime64[ns] y normalizadas
    for df, col in [
        (df_bonos_kpis,   "Fecha_Dia"),
        (df_bonos_diario, "Fecha_Dia"),
    ]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.normalize()

    # Filtrar series diarias (Timestamp vs Timestamp)
    kpis = df_bonos_kpis[(df_bonos_kpis["Fecha_Dia"] >= s) & (df_bonos_kpis["Fecha_Dia"] <= e)].copy()
    diario = df_bonos_diario[(df_bonos_diario["Fecha_Dia"] >= s) & (df_bonos_diario["Fecha_Dia"] <= e)].copy()

    # Totales del período
    bonos_tot = int(kpis["Bonos_Otorgados"].sum()) if not kpis.empty else 0
    monto_tot = float(kpis["Monto_Bonos"].sum()) if not kpis.empty else 0.0
    jugaron   = int(kpis["Jugaron_PostBono"].sum()) if not kpis.empty else 0
    agotaron  = int(kpis["Agotaron_Bono"].sum()) if not kpis.empty else 0
    recargaron = int(kpis["Recargaron_Luego"].sum()) if not kpis.empty else 0

    pct = lambda x: (x/bonos_tot*100) if bonos_tot > 0 else 0.0
    kpi_bonos_ot = f"{bonos_tot:,}"
    kpi_monto_bonos = f"${monto_tot:,.2f}"
    kpi_pct_jug = f"{pct(jugaron):.1f}%"
    kpi_pct_agot = f"{pct(agotaron):.1f}%"
    kpi_pct_rec = f"{pct(recargaron):.1f}%"

    # Gráfico 1: evolución (líneas $ y barras #)
    fig_evo = px.line(
        diario, x="Fecha_Dia", y="Monto_Bonos",
        title="Monto total de bonos por día", labels={"Monto_Bonos":"$"}
    )

    # Barras por tipo
    if not diario.empty:
        melted = diario.melt(
            id_vars=["Fecha_Dia"],
            value_vars=["Registro_Cant","Deposito_Cant"],
            var_name="Tipo", value_name="Cantidad"
        )
    else:
        melted = diario

    fig_tipo = px.bar(
        melted, x="Fecha_Dia", y="Cantidad", color="Tipo",
        title="Bonos por tipo (registro vs depósito)"
    )

    # Funnel comportamiento
    df_funnel = pd.DataFrame({
        "Etapa": ["Jugaron tras bono","Agotaron el bono","Recargaron después"],
        "Porcentaje": [pct(jugaron), pct(agotaron), pct(recargaron)]
    })
    fig_funnel = px.bar(df_funnel, x="Etapa", y="Porcentaje", title="Comportamiento posterior (%)")

    # Detalle (siempre Timestamp en la comparación)
    det = df_bonos_detalle.copy()
    if not det.empty:
        # Asegurar parse de la fecha del detalle (usa Fecha_Bono u otra)
        col_fecha_det = "Fecha_Bono" if "Fecha_Bono" in det.columns else "Fecha"
        det[col_fecha_det] = pd.to_datetime(det[col_fecha_det], errors="coerce").dt.normalize()
        det = det[(det[col_fecha_det] >= s) & (det[col_fecha_det] <= e)]

    data_table = det.to_dict("records")

    return (kpi_bonos_ot, kpi_monto_bonos, kpi_pct_jug, kpi_pct_agot, kpi_pct_rec,
            fig_evo, fig_tipo, fig_funnel, data_table)



if __name__ == "__main__":
    # Configuración para despliegue en Render
    port = int(os.environ.get("PORT", 10000))
    app.run(debug=False, host="0.0.0.0", port=port)
