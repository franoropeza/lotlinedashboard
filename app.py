import pandas as pd
import plotly.express as px
from dash import Dash, html, dcc, Input, Output
import dash_bootstrap_components as dbc

# Inicializar app
app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.title = "Dashboard Lotería de Salta"

# Cargar datasets
df_modo = pd.read_csv("modo_diario.csv", parse_dates=["Fecha_Dia"])
df_kpis = pd.read_csv("kpis.csv")
df_monto = pd.read_csv("recargas_monto.csv", parse_dates=["Fecha_Dia"])
df_cant = pd.read_csv("recargas_cant.csv", parse_dates=["Fecha_Dia"])
df_prom = pd.read_csv("deposito_promedio.csv", parse_dates=["Fecha_Dia"])
df_comp = pd.read_csv("comparativa_modo.csv")

# Filtros disponibles
fecha_min = df_monto["Fecha_Dia"].min()
fecha_max = df_monto["Fecha_Dia"].max()

# Layout
app.layout = dbc.Container([
    html.H1("📊 Dashboard Lotería de Salta", className="text-center my-4"),

    # Filtro de fecha
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

    # KPIs
    dbc.Row([
        dbc.Col(dbc.Card([
            dbc.CardHeader(kpi),
            dbc.CardBody(html.H4(f"{valor:,.0f}", className="card-title"))
        ], color="light")) for kpi, valor in zip(df_kpis["KPI"], df_kpis["Valor"])
    ], className="mb-4"),

    # Gráfico: $ por día por canal
    dbc.Row([
        dbc.Col(dcc.Graph(id="grafico_monto"))
    ]),
    # Gráfico: recargas por día por canal
    dbc.Row([
        dbc.Col(dcc.Graph(id="grafico_cant"))
    ]),
    # Gráfico: usuarios únicos MODO por día
    dbc.Row([
        dbc.Col(dcc.Graph(id="grafico_modo"))
    ]),
    # Gráfico: depósito promedio diario
    dbc.Row([
        dbc.Col(dcc.Graph(id="grafico_prom"))
    ]),
    # Gráfico: comparativa before/after
    dbc.Row([
        dbc.Col(dcc.Graph(
            figure=px.bar(
                df_comp,
                x="Periodo", y=["Depositos_$", "Recaudacion_$"],
                barmode="group",
                title="Antes/Después implementación MODO — Depósitos vs Recaudación",
                labels={"value": "$", "variable": "Tipo"}
            )
        ))
    ])
], fluid=True)

# Callbacks para actualizar por filtro de fecha
@app.callback(
    Output("grafico_monto", "figure"),
    Output("grafico_cant", "figure"),
    Output("grafico_modo", "figure"),
    Output("grafico_prom", "figure"),
    Input("filtro_fecha", "start_date"),
    Input("filtro_fecha", "end_date")
)
def actualizar_graficos(start, end):
    monto = df_monto[(df_monto["Fecha_Dia"] >= start) & (df_monto["Fecha_Dia"] <= end)]
    cant = df_cant[(df_cant["Fecha_Dia"] >= start) & (df_cant["Fecha_Dia"] <= end)]
    modo = df_modo[(df_modo["Fecha_Dia"] >= start) & (df_modo["Fecha_Dia"] <= end)]
    prom = df_prom[(df_prom["Fecha_Dia"] >= start) & (df_prom["Fecha_Dia"] <= end)]

    fig_monto = px.line(monto, x="Fecha_Dia", y=["MODO", "Retail"],
                        title="$ por día por canal", labels={"value": "$", "variable": "Canal"})
    fig_cant = px.bar(cant, x="Fecha_Dia", y=["MODO", "Retail"],
                      title="Recargas por día por canal", labels={"value": "Cantidad", "variable": "Canal"})
    fig_modo = px.line(modo, x="Fecha_Dia", y="Usuarios_Unicos",
                       title="Usuarios únicos MODO por día")
    fig_prom = px.line(prom, x="Fecha_Dia", y=["MODO", "Retail"],
                       title="Depósito promedio diario — MODO vs Retail", labels={"value": "$", "variable": "Canal"})

    return fig_monto, fig_cant, fig_modo, fig_prom

if __name__ == "__main__":
app.run(debug=True)
