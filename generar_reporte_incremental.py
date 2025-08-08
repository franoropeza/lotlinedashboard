#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Genera y actualiza el reporte de billeteras de forma *incremental*:
1) Lee solo los .xls nuevos/modificados de data/
2) Actualiza datasets/movimientos.parquet y manifest.csv
3) Vuelve a crear reporte_movimientos.xlsx y la plantilla con pivots
"""

# ================== IMPORTS Y CONFIG ==================
from pathlib import Path
from datetime import datetime
import warnings
import shutil
import unicodedata
import os
import pandas as pd
import numpy as np
import xlwings as xw  # Importamos la librería xlwings

warnings.filterwarnings("ignore", message="Workbook contains no default style")

# carpetas
ROOT = Path(__file__).parent
DATA_DIR = ROOT / "data"          # llegan los .xls diarios
PROC_DIR = ROOT / "processed"     # .xls ya integrados
DS_DIR = ROOT / "datasets"        # parquet + manifest
csv_dir = ROOT / "csv_dashboard"  # para CSVs de salida

PROC_DIR.mkdir(exist_ok=True)
DS_DIR.mkdir(exist_ok=True)
csv_dir.mkdir(exist_ok=True)

MANIFEST_FILE = DS_DIR / "manifest.csv"
MASTER_FILE = DS_DIR / "movimientos.parquet"

# salidas
SALIDA_ANALITICO = ROOT / "reporte_movimientos.xlsx"
USUARIOS_FILE = ROOT / "REPORTE-A-MEDIDA-USUARIOSACTIVOS.xlsx"  # archivo con datos de usuario

SHARE_DIR = ROOT / "public"       # carpeta compartida
SHARE_DIR.mkdir(exist_ok=True)

# hitos
FECHA_LANZ_JUEGOS = pd.Timestamp("2025-04-14")
FECHA_MODO_FULL = pd.Timestamp("2025-07-07")


# ================== FUNCIONES BASE ==================
def normalizar(txt: str) -> str:
    """Normaliza un texto, eliminando acentos y convirtiendo a minúsculas."""
    if pd.isna(txt):
        return ""
    txt = unicodedata.normalize("NFKD", str(txt))
    return "".join(c for c in txt if not unicodedata.combining(c)).lower()

def leer_movimientos(archivo: Path) -> pd.DataFrame | None:
    """Lee un .xls y devuelve un dataframe normalizado (o None si no detecta encabezado)."""
    crudo = pd.read_excel(archivo, header=None)
    header_mask = crudo.apply(
        lambda fila: fila.apply(normalizar).str.contains("tipo mov", na=False).any(),
        axis=1,
    )
    if not header_mask.any():
        print(f"⚠️  Encabezado no encontrado en {archivo.name} — omitido")
        return None
    header_idx = header_mask.idxmax()

    df = pd.read_excel(
        archivo, header=header_idx,
        usecols=["Nro. Transacción", "Fecha", "Tipo Mov.", "Documento", "Movimiento", "Importe"]
    )
    df["Fecha"] = pd.to_datetime(df["Fecha"], dayfirst=True, errors="coerce")
    df["Importe"] = (
        df["Importe"].astype(str)
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
        .astype(float)
    )
    return df

def clasificar_canal(movimiento: str) -> str:
    """Clasifica un movimiento como MODO o Retail."""
    return "MODO" if "modo" in normalizar(movimiento) else "Retail"

def get_mtime(p: Path) -> int:
    """Obtiene el tiempo de última modificación de un archivo."""
    return int(p.stat().st_mtime)

# ================== BLOQUE INCREMENTAL ==================
print("🔄  Paso 1: identificar archivos nuevos…")
manifest = pd.read_csv(MANIFEST_FILE) if MANIFEST_FILE.exists() else pd.DataFrame(columns=["archivo", "mod_time"])
pendientes = []
for f in DATA_DIR.glob("*.xls"):
    mt = get_mtime(f)
    fila = manifest.loc[manifest["archivo"] == f.name]
    if fila.empty or fila.iloc[0]["mod_time"] != mt:
        pendientes.append((f, mt))

nuevos_df = []
if pendientes:
    for f, mt in pendientes:
        print("   • Procesando", f.name)
        df_tmp = leer_movimientos(f)
        if df_tmp is not None:
            nuevos_df.append(df_tmp)
            # mover a processed/YYYY-MM/
            dest_dir = PROC_DIR / f"{df_tmp['Fecha'].dt.to_period('M').iloc[0]}"
            dest_dir.mkdir(exist_ok=True)
            shutil.move(str(f), dest_dir / f.name)
            # actualizar manifest
            manifest = manifest[manifest["archivo"] != f.name]
            manifest.loc[len(manifest)] = [f.name, mt]

# actualizar parquet maestro
if nuevos_df:
    df_new = pd.concat(nuevos_df, ignore_index=True)
    if MASTER_FILE.exists():
        df_old = pd.read_parquet(MASTER_FILE)
        data_total = pd.concat([df_old, df_new], ignore_index=True)
        data_total.drop_duplicates(subset=["Nro. Transacción"], inplace=True)
    else:
        data_total = df_new
    data_total.to_parquet(MASTER_FILE, index=False)
    manifest.to_csv(MANIFEST_FILE, index=False)
    print(f"✅ Agregados {len(df_new)} movimientos nuevos.")
else:
    if not MASTER_FILE.exists():
        raise RuntimeError("No hay parquet maestro y no se encontraron .xls para procesar.")
    data_total = pd.read_parquet(MASTER_FILE)
    print("✅ Dataset maestro ya estaba al día.")

# trabajar con copia para no modificar el maestro accidentalmente
data = data_total.copy()
print(f"📊  Movimientos totales en dataset: {len(data):,}")

# ========= (lo que sigue es *idéntico* a la última versión estable) =========
# ========= APUESTAS =========
apuestas = data[data["Tipo Mov."].str.contains("apuesta|jugada", case=False, na=False)].copy()
apuestas["AñoMes"] = apuestas["Fecha"].dt.to_period("M")
apuestas["Fecha_Dia"] = apuestas["Fecha"].dt.date
apuestas["Juego"] = (
    apuestas["Movimiento"].str.replace(r"(?i)jugada\s*-\s*", "", regex=True).str.strip()
)
apuestas["Juego_norm"] = apuestas["Juego"].apply(normalizar)

dias_map = {0: "Lunes", 1: "Martes", 2: "Miércoles", 3: "Jueves", 4: "Viernes", 5: "Sábado", 6: "Domingo"}
apuestas["Dia_Sem"] = apuestas["Fecha"].dt.weekday.map(dias_map)

# ========= CLIENTE_MES =========
cliente_mes = (
    apuestas.groupby(["Documento", "AñoMes", "Juego"])
    .agg(Bets=("Importe", "count"), Gastado=("Importe", "sum"))
    .reset_index()
)

# ========= TOP GAMES =========
top_games_total = (
    apuestas.groupby("Juego")
    .agg(Bets_Totales=("Importe", "count"),
         Jugadores_Unicos=("Documento", "nunique"),
         Gastado=("Importe", "sum"))
    .sort_values("Bets_Totales", ascending=False)
    .reset_index()
)

top_games_mes = (
    apuestas.groupby(["AñoMes", "Juego"])
    .agg(Bets_Mes=("Importe", "count"),
         Jugadores_Mes=("Documento", "nunique"),
         Gastado_Mes=("Importe", "sum"))
    .reset_index()
    .sort_values(["AñoMes", "Bets_Mes"], ascending=[False, False])
)

# ========= PESTAÑAS POR JUEGO =========
GAME_PATTERNS = {
    "Tombo_Express": r"tombo express",
    "Tombola": r"t(?:o|ó)mbola",
    "Quini6": r"quini\s*6",
    "Loto_Plus": r"loto(?:\s*plus)?",
}
game_summaries = {}
for sheet, pattern in GAME_PATTERNS.items():
    mask = apuestas["Juego_norm"].str.contains(pattern, regex=True, na=False)
    tmp = apuestas.loc[mask]
    summary = (
        tmp.groupby("Documento")
        .agg(Bets=("Importe", "count"), Gastado=("Importe", "sum"))
        .sort_values("Gastado", ascending=False)
        .reset_index()
    )
    game_summaries[sheet] = summary

# ========= RECARGAS / RETIROS / PREMIOS =========
mask_carga = data["Tipo Mov."].str.contains(
    r"carga|dep(?:o|ó)sito", case=False, regex=True, na=False
)
cargas = data.loc[mask_carga].copy()
cargas["Canal"] = cargas["Movimiento"].apply(clasificar_canal)
cargas["Metodo"] = np.where(cargas["Canal"] == "MODO", "MODO", "Retail")
cargas["Fecha_Dia"] = cargas["Fecha"].dt.date
cargas["Hora"] = cargas["Fecha"].dt.hour

recargas_diario_canal = (
    cargas.groupby(["Fecha_Dia", "Canal"])
    .agg(Recargas=("Importe", "count"),
         Monto=("Importe", "sum"),
         Usuarios_Unicos=("Documento", "nunique"))
    .reset_index()
)

recargas_dia_monto = (
    recargas_diario_canal.pivot(index="Fecha_Dia", columns="Canal", values="Monto")
    .fillna(0.0)
    .reset_index()
    .sort_values("Fecha_Dia")
)
recargas_dia_cant = (
    recargas_diario_canal.pivot(index="Fecha_Dia", columns="Canal", values="Recargas")
    .fillna(0)
    .reset_index()
    .sort_values("Fecha_Dia")
)

modo_diario = (
    cargas.loc[cargas["Canal"] == "MODO"]
    .groupby("Fecha_Dia")
    .agg(
        Recargas_MODO=("Importe", "count"),
        Monto_MODO=("Importe", "sum"),
        Usuarios_Unicos=("Documento", "nunique"),
    )
    .reset_index()
    .sort_values("Fecha_Dia", ascending=False)
)

mask_retiro = data["Tipo Mov."].str.contains("retiro|transferencia salida", case=False, na=False)
retiros = data.loc[mask_retiro].copy()
retiros["Fecha_Dia"] = retiros["Fecha"].dt.date
retiros_diario = (
    retiros.groupby("Fecha_Dia")
    .agg(Retiros=("Importe", "count"),
         Monto_Retirado=("Importe", "sum"),
         Clientes_Unicos=("Documento", "nunique"))
    .reset_index()
    .sort_values("Fecha_Dia", ascending=False)
)

mask_premio = data["Tipo Mov."].str.contains("premio", case=False, na=False)
premios = data.loc[mask_premio].copy()
premios_resumen = (
    premios.groupby("Documento")
    .agg(Premios_Cobrados=("Importe", "count"), Monto_Premios=("Importe", "sum"))
    .sort_values("Monto_Premios", ascending=False)
    .reset_index()
)

# ========= Juego por día (detalle) =========
juego_dia_detalle = (
    apuestas.groupby(["Fecha_Dia", "Juego"])
    .agg(Bets=("Importe", "count"),
         Usuarios_Unicos_Dia=("Documento", "nunique"),
         Gastado_Dia=("Importe", "sum"))
    .reset_index()
    .sort_values(["Fecha_Dia", "Juego"])
)

dia_totales = (
    apuestas.groupby("Dia_Sem")
    .agg(Bets=("Importe", "count"),
         Usuarios_Unicos=("Documento", "nunique"))
    .reset_index()
)

# ========= Retención MODO =========
primer_mov_total = (
    data.sort_values("Fecha")
    .groupby("Documento", as_index=False)
    .first()
    .rename(columns={"Fecha": "Fecha_PrimerMov"})
)

modo_all = cargas.loc[cargas["Canal"] == "MODO"].copy()
primera_modo = (
    modo_all.sort_values("Fecha")
    .groupby("Documento", as_index=False)
    .first()
    .rename(columns={"Fecha": "Fecha_Corte"})
)
retencion_base = primera_modo.merge(primer_mov_total, on="Documento", how="left")
retencion_base["Es_Nuevo"] = retencion_base["Fecha_PrimerMov"] == retencion_base["Fecha_Corte"]

apuestas_only = apuestas[["Documento", "Fecha"]].copy()
joined = retencion_base[["Documento", "Fecha_Corte"]].merge(apuestas_only, on="Documento", how="left")
joined["Posterior"] = joined["Fecha"] > joined["Fecha_Corte"]
joined["Dia_Siguiente"] = (joined["Fecha"] > joined["Fecha_Corte"]) & \
                          (joined["Fecha"] <= joined["Fecha_Corte"] + pd.Timedelta(days=1))
joined["Mes_Siguiente"] = (joined["Fecha"] > joined["Fecha_Corte"]) & \
                          (joined["Fecha"] <= joined["Fecha_Corte"] + pd.Timedelta(days=30))

flags = (
    joined.groupby("Documento")
    .agg(Jugo_Posterior=("Posterior", "any"),
         Jugo_Dia_Sig=("Dia_Siguiente", "any"),
         Jugo_Mes_Sig=("Mes_Siguiente", "any"))
    .reset_index()
)
retencion_modo = retencion_base.merge(flags, on="Documento", how="left").fillna(False)
print(f"▶︎ Cantidad de usuarios NUEVOS que cargaron con MODO: {int(retencion_modo['Es_Nuevo'].sum())}")

# ========= Crecimiento / Hitos =========
primer_mov_total["AñoMes_PrimerMov"] = primer_mov_total["Fecha_PrimerMov"].dt.to_period("M")
usuarios_mes = (
    primer_mov_total.groupby("AñoMes_PrimerMov")
    .agg(Nuevos=("Documento", "nunique"))
    .reset_index()
    .sort_values("AñoMes_PrimerMov")
)
usuarios_mes["Acumulado"] = usuarios_mes["Nuevos"].cumsum()
activos_mes = (
    apuestas.groupby(apuestas["Fecha"].dt.to_period("M"))["Documento"]
    .nunique()
    .reset_index(name="Jugadores_Activos_Mes")
    .rename(columns={"Fecha": "AñoMes"})
)
activos_mes["AñoMes"] = activos_mes["AñoMes"].astype(str)
usuarios_mes["AñoMes"] = usuarios_mes["AñoMes_PrimerMov"].astype(str)
usuarios_mes = usuarios_mes.merge(activos_mes, on="AñoMes", how="left")
usuarios_mes = usuarios_mes[["AñoMes", "Nuevos", "Acumulado", "Jugadores_Activos_Mes"]]

nuevos_desde_juegos = primer_mov_total.loc[
    primer_mov_total["Fecha_PrimerMov"] >= FECHA_LANZ_JUEGOS, "Documento"
].nunique()
jugadores_desde_juegos = apuestas.loc[
    apuestas["Fecha"] >= FECHA_LANZ_JUEGOS, "Documento"
].nunique()
mask_newgames = (
    (apuestas["Fecha"] >= FECHA_LANZ_JUEGOS) &
    apuestas["Juego_norm"].str.contains(r"(?:quini\s*6|loto(?:\s*plus)?)", regex=True, na=False)
)
jugadores_quini_loto_desde = apuestas.loc[mask_newgames, "Documento"].nunique()

modo_post_docs = set(
    cargas.loc[(cargas["Fecha"] >= FECHA_MODO_FULL) & (cargas["Canal"] == "MODO"), "Documento"].unique()
)
usuarios_modo_desde = len(modo_post_docs)
jugadores_post_modo_docs = set(apuestas.loc[apuestas["Fecha"] >= FECHA_MODO_FULL, "Documento"].unique())
jugadores_post_modo = len(jugadores_post_modo_docs)
jugadores_modo_y_jugaron = len(modo_post_docs & jugadores_post_modo_docs)

usuarios_hitos = pd.DataFrame({
    "Concepto": [
        "Nuevos >= 2025-04-14 (cualquier mov.)",
        "Jugadores >= 2025-04-14 (cualquier apuesta)",
        "Apostaron Quini/Loto >= 2025-04-14",
        "Recargaron MODO >= 2025-07-07",
        "Jugadores >= 2025-07-07 (cualquier apuesta)",
        "Recargaron MODO y jugaron >= 2025-07-07",
    ],
    "Valor": [
        nuevos_desde_juegos,
        jugadores_desde_juegos,
        jugadores_quini_loto_desde,
        usuarios_modo_desde,
        jugadores_post_modo,
        jugadores_modo_y_jugaron,
    ],
})

# ========= Comparativa Before/After 07/07 =========
dep_before = cargas.loc[cargas["Fecha"] < FECHA_MODO_FULL, "Importe"].sum()
dep_after = cargas.loc[cargas["Fecha"] >= FECHA_MODO_FULL, "Importe"].sum()
rec_before = apuestas.loc[apuestas["Fecha"] < FECHA_MODO_FULL, "Importe"].sum()
rec_after = apuestas.loc[apuestas["Fecha"] >= FECHA_MODO_FULL, "Importe"].sum()

comparativa_modo = pd.DataFrame({
    "Periodo": ["Before 07/07/2025", "After 07/07/2025"],
    "Depositos_$": [dep_before, dep_after],
    "Recaudacion_$": [rec_before, rec_after],
})

# ========= KPIs Resumen =========
promedio_deposito = cargas["Importe"].mean() if len(cargas) else 0.0
cant_unicos_total = data["Documento"].nunique()
cant_unicos_apuestan = apuestas["Documento"].nunique()
cant_recargas_unicas = cargas["Documento"].nunique()

agg_canal = (
    cargas.groupby("Canal")
    .agg(Recargas=("Importe", "count"),
         Usuarios_Unicos=("Documento", "nunique"),
         Monto=("Importe", "sum"))
    .reset_index()
)
recargas_modo = int(agg_canal.loc[agg_canal["Canal"] == "MODO", "Recargas"].sum())
recargas_retail = int(agg_canal.loc[agg_canal["Canal"] == "Retail", "Recargas"].sum())
monto_modo = float(agg_canal.loc[agg_canal["Canal"] == "MODO", "Monto"].sum())
monto_retail = float(agg_canal.loc[agg_canal["Canal"] == "Retail", "Monto"].sum())

resumen_kpis = pd.DataFrame({
    "KPI": [
        "Promedio depósito $",
        "Usuarios únicos (cualquier mov.)",
        "Usuarios únicos apostadores",
        "Usuarios únicos que recargaron",
        "Recargas - MODO",
        "Recargas - Retail",
        "Monto MODO $",
        "Monto Retail $",
    ],
    "Valor": [
        promedio_deposito,
        cant_unicos_total,
        cant_unicos_apuestan,
        cant_recargas_unicas,
        recargas_modo,
        recargas_retail,
        monto_modo,
        monto_retail,
    ]
})

# ========= PROCESAMIENTO DE DATOS DE USUARIO (SI EXISTE EL ARCHIVO) =========
# Inicializar DataFrames para evitar NameError si el archivo no existe o falla
top10_contactos = pd.DataFrame()
usuarios_nuevos_modo = pd.DataFrame()
usuarios_reactivados_modo = pd.DataFrame()
top10_por_juego_con_datos = {}

if USUARIOS_FILE.exists():
    print(f"📄 Procesando archivo de usuarios: {USUARIOS_FILE.name}")
    try:
        usuarios = pd.read_excel(USUARIOS_FILE)
        usuarios.columns = [str(c).strip() for c in usuarios.columns]

        # --- Normalizar columnas de DNI y Fecha ---
        dni_col = next((c for c in usuarios.columns if "doc" in c.lower()), None)
        if dni_col:
            usuarios.rename(columns={dni_col: "Documento"}, inplace=True)
        else:
            raise ValueError("No se encontró columna de Documento/DNI en el archivo de usuarios.")

        fecha_col = next((c for c in usuarios.columns if "fecha" in c.lower() and "alta" in c.lower()), None)
        if fecha_col:
            usuarios.rename(columns={fecha_col: "Fecha_Alta"}, inplace=True)
        else:
            raise ValueError("No se encontró columna de Fecha Alta en el archivo de usuarios.")

        usuarios["Documento"] = pd.to_numeric(
            usuarios["Documento"].astype(str).str.extract(r"(\d+)")[0].str.lstrip("0"),
            errors="coerce"
        )
        usuarios["Fecha_Alta"] = pd.to_datetime(usuarios["Fecha_Alta"], dayfirst=True, errors="coerce")
        usuarios = usuarios.dropna(subset=["Documento", "Fecha_Alta"])
        usuarios_unicos = usuarios.drop_duplicates(subset="Documento", keep="last")

        # --- Top 10 Contactos (General) ---
        print("   • Generando Top 10 contactos...")
        jugadas_por_doc = (
            apuestas.groupby("Documento")
            .agg(Bets_Total=("Importe", "count"), Gastado_Total=("Importe", "sum"))
            .reset_index()
        )
        top10_contactos = (
            jugadas_por_doc.merge(usuarios_unicos, how="inner", on="Documento")
            .sort_values("Bets_Total", ascending=False)
            .head(10)
        )

        # --- Usuarios Nuevos y Reactivados por MODO ---
        print("   • Generando reportes de usuarios nuevos y reactivados por MODO...")
        usuarios_nuevos_modo = usuarios_unicos[usuarios_unicos["Fecha_Alta"] >= FECHA_MODO_FULL].copy()
        usuarios_nuevos_modo = usuarios_nuevos_modo[["Documento", "Fecha_Alta", "Usuario", "Correo"]]
        print(f"   ✅ Encontrados {len(usuarios_nuevos_modo)} usuarios nuevos desde el lanzamiento de MODO.")

        antiguos = usuarios_unicos[usuarios_unicos["Fecha_Alta"].dt.year.between(2021, 2024)].copy()
        cargas_modo_docs_antes = set(cargas.loc[(cargas["Fecha"] < FECHA_MODO_FULL) & (cargas["Canal"] == "MODO"), "Documento"])
        cargas_modo_docs_despues = set(cargas.loc[cargas["Fecha"] >= FECHA_MODO_FULL, "Documento"])
        
        reactivados_docs = set(antiguos["Documento"]) & (cargas_modo_docs_despues - cargas_modo_docs_antes)
        usuarios_reactivados_modo = antiguos[antiguos["Documento"].isin(reactivados_docs)].copy()
        
        if not usuarios_reactivados_modo.empty:
            primer_modo_post = cargas.loc[cargas["Fecha"] >= FECHA_MODO_FULL].sort_values("Fecha").drop_duplicates("Documento")
            usuarios_reactivados_modo = usuarios_reactivados_modo.merge(
                primer_modo_post[["Documento", "Fecha"]].rename(columns={"Fecha": "Fecha_Reactivacion"}),
                on="Documento",
                how="left"
            )
        print(f"   ✅ Encontrados {len(usuarios_reactivados_modo)} usuarios reactivados con MODO.")

        # --- Top 10 por Juego (con datos de contacto) ---
        print("   • Generando Top 10 por juego...")
        for juego, df_summary in game_summaries.items():
            df = df_summary.copy()
            df["Documento"] = pd.to_numeric(df["Documento"], errors="coerce").astype("Int64")
            merged = df.merge(usuarios_unicos, on="Documento", how="inner")
            top10_por_juego_con_datos[juego] = merged.sort_values("Gastado", ascending=False).head(10)

    except Exception as e:
        print(f"⚠️  Error al procesar el archivo de usuarios: {e}")

# ========= Exportar a CSV para Dashboard =========
print("📤 Exportando archivos CSV para el dashboard...")

def export_df_to_csv(df, filename):
    """Función auxiliar para exportar un DataFrame a CSV."""
    try:
        if not df.empty:
            df.to_csv(csv_dir / filename, index=False)
            print(f"   ✅ CSV generado: {filename}")
        else:
            print(f"   ℹ️  DataFrame para {filename} está vacío, no se genera CSV.")
    except Exception as e:
        print(f"   ⚠️  Error al exportar {filename}: {e}")

# --- Exportaciones principales ---
export_df_to_csv(modo_diario, "modo_diario.csv")
export_df_to_csv(recargas_dia_monto, "recargas_monto.csv")
export_df_to_csv(recargas_dia_cant, "recargas_cant.csv")
export_df_to_csv(comparativa_modo, "comparativa_modo.csv")
export_df_to_csv(resumen_kpis, "kpis.csv")
export_df_to_csv(top_games_mes, "jugadores_unicos_por_juego.csv")

total_juegos_mes = (
    apuestas.groupby(["AñoMes", "Juego"])["Importe"]
    .count()
    .rename("Total_Bets")
    .reset_index()
)
export_df_to_csv(total_juegos_mes, "total_juegos_mes.csv")

# --- Exportaciones de datos de usuario (si se generaron) ---
export_df_to_csv(usuarios_nuevos_modo, "nuevos_modo.csv")
export_df_to_csv(usuarios_reactivados_modo, "reactivados_modo.csv")
export_df_to_csv(top10_contactos, "top10_contactos.csv")

for juego, df_top10 in top10_por_juego_con_datos.items():
    export_df_to_csv(df_top10, f"top10_{juego.lower()}.csv")

# --- Cálculo y exportación de depósito promedio ---
try:
    if not recargas_dia_monto.empty and not recargas_dia_cant.empty:
        prom = recargas_dia_monto.copy().set_index("Fecha_Dia")
        cant = recargas_dia_cant.set_index("Fecha_Dia")
        for col in ["MODO", "Retail"]:
            if col in prom.columns and col in cant.columns:
                prom[col] = prom[col] / cant[col]
        export_df_to_csv(prom.reset_index(), "deposito_promedio.csv")
except Exception as e:
    print(f"   ⚠️  Error al generar deposito_promedio.csv: {e}")

# ========= Guardar DataFrames en un archivo .xlsx plano =========
print(f"💾 Guardando reporte analítico completo en: {SALIDA_ANALITICO.name}")
with pd.ExcelWriter(SALIDA_ANALITICO, engine="openpyxl") as writer:
    data.to_excel(writer, sheet_name="Base_Movimientos", index=False)
    resumen_kpis.to_excel(writer, sheet_name="Resumen_Datos", index=False)
    cliente_mes.to_excel(writer, sheet_name="Cliente_Mes", index=False)
    top_games_total.to_excel(writer, sheet_name="Top_Games_Total", index=False)
    top_games_mes.to_excel(writer, sheet_name="Top_Games_Mes", index=False)
    recargas_diario_canal.to_excel(writer, sheet_name="Recargas_Diario", index=False)
    modo_diario.to_excel(writer, sheet_name="MODO_Diario", index=False)
    retiros_diario.to_excel(writer, sheet_name="Retiros_Diario", index=False)
    premios_resumen.to_excel(writer, sheet_name="Ganadores", index=False)
    juego_dia_detalle.to_excel(writer, sheet_name="Juego_Dia_Detalle", index=False)
    dia_totales.to_excel(writer, sheet_name="Dia_Totales", index=False)
    retencion_modo.to_excel(writer, sheet_name="Retencion_MODO", index=False)
    usuarios_mes.to_excel(writer, sheet_name="Usuarios_Mes", index=False)
    usuarios_hitos.to_excel(writer, sheet_name="Usuarios_Hitos", index=False)
    comparativa_modo.to_excel(writer, sheet_name="Comparativa_MODO", index=False)
    total_juegos_mes.to_excel(writer, sheet_name="Total_Juegos_Mes", index=False)
    # Hojas con datos de usuario
    if not top10_contactos.empty:
        top10_contactos.to_excel(writer, sheet_name="Top10_Contactos", index=False)
    if not usuarios_nuevos_modo.empty:
        usuarios_nuevos_modo.to_excel(writer, sheet_name="Nuevos_MODO", index=False)
    if not usuarios_reactivados_modo.empty:
        usuarios_reactivados_modo.to_excel(writer, sheet_name="Reactivados_MODO", index=False)
    for juego, df_top10 in top10_por_juego_con_datos.items():
        if not df_top10.empty:
            df_top10.to_excel(writer, sheet_name=f"Top10_{juego}", index=False)

print("✅ Archivo Excel generado:", SALIDA_ANALITICO.name)
print("🏁  Proceso incremental terminado.")
