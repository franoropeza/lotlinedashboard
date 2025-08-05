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
from datetime import datetime, timedelta
import warnings, shutil, unicodedata, os, time
import pandas as pd
import numpy as np
import xlwings as xw  # Importamos la librería xlwings

warnings.filterwarnings("ignore", message="Workbook contains no default style")

## carpetas
ROOT          = Path(__file__).parent
DATA_DIR      = ROOT / "data"        # llegan los .xls diarios
PROC_DIR      = ROOT / "processed"   # .xls ya integrados
DS_DIR        = ROOT / "datasets"    # parquet + manifest
csv_dir       = ROOT / "csv_dashboard" # para CSVs de salida

PROC_DIR.mkdir(exist_ok=True)
DS_DIR.mkdir(exist_ok=True)
csv_dir.mkdir(exist_ok=True)

MANIFEST_FILE = DS_DIR / "manifest.csv"
MASTER_FILE   = DS_DIR / "movimientos.parquet"

## salidas
SALIDA_ANALITICO = ROOT / "reporte_movimientos.xlsx"
TEMPLATE_PATH    = ROOT / "Plantilla-Reporte.xlsm"   # con macros / pivots
SALIDA_XLWINGS   = ROOT / f"ReporteMovimientos-{datetime.now():%Y%m%d}.xlsm"
USUARIOS_FILE    = ROOT / "REPORTE-A-MEDIDA-USUARIOSACTIVOS.xlsx"      # archivo con datos de usuario

SHARE_DIR        = ROOT / "public"                  # carpeta compartida
SHARE_DIR.mkdir(exist_ok=True)
SALIDA_PUBLICA   = SHARE_DIR / "Dashboard-Billeteras.xlsm"
ENABLE_XLWINGS   = True

# hitos
FECHA_LANZ_JUEGOS = pd.Timestamp("2025-04-14")
FECHA_MODO_FULL   = pd.Timestamp("2025-07-07")

# ================== FUNCIONES BASE ==================
def normalizar(txt: str) -> str:
    """Normaliza un texto, eliminando acentos y convirtiendo a minúsculas."""
    if pd.isna(txt): return ""
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
        usecols=["Nro. Transacción","Fecha","Tipo Mov.","Documento","Movimiento","Importe"]
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
manifest = pd.read_csv(MANIFEST_FILE) if MANIFEST_FILE.exists() else pd.DataFrame(columns=["archivo","mod_time"])
pendientes = []
for f in DATA_DIR.glob("*.xls"):
    mt = get_mtime(f)
    fila = manifest.loc[manifest["archivo"] == f.name]
    if fila.empty or fila.iloc[0]["mod_time"] != mt:
        pendientes.append((f, mt))

nuevos_df = []
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
## ========= APUESTAS =========
apuestas = data[data["Tipo Mov."].str.contains("apuesta|jugada", case=False, na=False)].copy()
apuestas["AñoMes"]    = apuestas["Fecha"].dt.to_period("M")
apuestas["Fecha_Dia"] = apuestas["Fecha"].dt.date
apuestas["Juego"] = (
    apuestas["Movimiento"].str.replace(r"(?i)jugada\s*-\s*", "", regex=True).str.strip()
)
apuestas["Juego_norm"] = apuestas["Juego"].apply(normalizar)

dias_map = {0:"Lunes",1:"Martes",2:"Miércoles",3:"Jueves",4:"Viernes",5:"Sábado",6:"Domingo"}
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
    "Tombola":       r"t(?:o|ó)mbola",
    "Quini6":        r"quini\s*6",
    "Loto_Plus":     r"loto(?:\s*plus)?",
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
cargas["Canal"]     = cargas["Movimiento"].apply(clasificar_canal)
cargas["Metodo"]    = np.where(cargas["Canal"]=="MODO", "MODO", "Retail")
cargas["Fecha_Dia"] = cargas["Fecha"].dt.date
cargas["Hora"]      = cargas["Fecha"].dt.hour

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
              Recargas_MODO   = ("Importe", "count"),
              Monto_MODO      = ("Importe", "sum"),
              Usuarios_Unicos = ("Documento", "nunique"),
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
joined["Posterior"]     = joined["Fecha"] >  joined["Fecha_Corte"]
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
dep_after  = cargas.loc[cargas["Fecha"] >= FECHA_MODO_FULL, "Importe"].sum()
rec_before = apuestas.loc[apuestas["Fecha"] < FECHA_MODO_FULL, "Importe"].sum()
rec_after  = apuestas.loc[apuestas["Fecha"] >= FECHA_MODO_FULL, "Importe"].sum()

comparativa_modo = pd.DataFrame({
    "Periodo": ["Before 07/07/2025", "After 07/07/2025"],
    "Depositos_$": [dep_before, dep_after],
    "Recaudacion_$": [rec_before, rec_after],
})

# ========= KPIs Resumen =========
promedio_deposito    = cargas["Importe"].mean() if len(cargas) else 0.0
cant_unicos_total    = data["Documento"].nunique()
cant_unicos_apuestan = apuestas["Documento"].nunique()
cant_recargas_unicas = cargas["Documento"].nunique()

agg_canal = (
    cargas.groupby("Canal")
          .agg(Recargas=("Importe", "count"),
               Usuarios_Unicos=("Documento", "nunique"),
               Monto=("Importe", "sum"))
          .reset_index()
)
recargas_modo   = int(agg_canal.loc[agg_canal["Canal"]=="MODO","Recargas"].sum())
recargas_retail = int(agg_canal.loc[agg_canal["Canal"]=="Retail","Recargas"].sum())
monto_modo      = float(agg_canal.loc[agg_canal["Canal"]=="MODO","Monto"].sum())
monto_retail    = float(agg_canal.loc[agg_canal["Canal"]=="Retail","Monto"].sum())

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

# Inicializar DataFrames para evitar NameError
top10_contactos = None
usuarios_nuevos_modo = None
usuarios_reactivados_modo = None
top10_por_juego_con_datos = {}


# ========= Top10 contactos (opcional) =========
if USUARIOS_FILE.exists():
    try:
        # Cargar usuarios, limpiar y normalizar
        usuarios_raw = pd.read_excel(USUARIOS_FILE)
        usuarios_raw.columns = usuarios_raw.columns.str.strip()
        # Normalizar el nombre de la columna "Fecha_Alta" si es necesario
        if "Fecha_Alta" not in usuarios_raw.columns and "Fecha Alta" in usuarios_raw.columns:
            usuarios_raw.rename(columns={"Fecha Alta": "Fecha_Alta"}, inplace=True)
        # Intentar parsear las fechas con el método dayfirst para mayor robustez
        usuarios_raw["Fecha_Alta"] = pd.to_datetime(usuarios_raw["Fecha_Alta"], dayfirst=True, errors="coerce")
        # Copia para no modificar el df original
        usuarios = usuarios_raw.copy()

        if "DNI" not in usuarios.columns:
            cand = [c for c in usuarios.columns if "dni" in c.lower() or "doc" in c.lower()]
            if cand:
                usuarios.rename(columns={cand[0]: "DNI"}, inplace=True)
            else:
                raise KeyError("No se encontró columna DNI en usuarios_limpio.csv")
        usuarios["DNI"] = pd.to_numeric(usuarios["DNI"], errors="coerce")
        usuarios = usuarios.dropna(subset=["DNI"]).drop_duplicates(subset="DNI", keep="last")

        jugadas_por_doc = (
            apuestas.groupby("Documento")
                    .agg(Bets_Total=("Importe", "count"),
                         Gastado_Total=("Importe", "sum"))
                    .reset_index()
        )

        top10_contactos = (
            jugadas_por_doc.merge(usuarios, how="inner", left_on="Documento", right_on="DNI")
                           .sort_values("Bets_Total", ascending=False)
                           .head(10)
        )
    except Exception as e:
        print(f"⚠️  No se pudo generar Top10_Contactos: {e}")

# ==============================================
# Detectar usuarios NUEVOS y REACTIVADOS con MODO (desde 7/7)
# ==============================================
# Primero, generamos el CSV con los movimientos MODO para el siguiente paso
try:
    modo_movimientos = cargas[cargas["Canal"] == "MODO"][["Documento", "Fecha", "Importe"]].copy()
    modo_movimientos.to_csv(csv_dir / "movimientos_modo.csv", index=False)
    print("✅ CSV generado: movimientos_modo.csv")
except Exception as e:
    print(f"⚠️ Error al generar movimientos_modo.csv: {e}")

# Ahora procesamos los usuarios nuevos y reactivados
if USUARIOS_FILE.exists():
    try:
        # Cargar usuarios
        usuarios = pd.read_excel(USUARIOS_FILE)
        usuarios.columns = usuarios.columns.str.strip()
        # Normalizar el nombre de la columna "Fecha_Alta" si es necesario
        if "Fecha_Alta" not in usuarios.columns and "Fecha Alta" in usuarios.columns:
            usuarios.rename(columns={"Fecha Alta": "Fecha_Alta"}, inplace=True)
        usuarios["Fecha_Alta"] = pd.to_datetime(usuarios["Fecha_Alta"], dayfirst=True, errors="coerce")
        
        # Normalizar DNI
        usuarios["Documento"] = pd.to_numeric(
            usuarios["Documento"].astype(str).str.extract(r"(\d+)")[0].str.lstrip("0"),
            errors="coerce"
        )
        usuarios = usuarios.dropna(subset=["Documento"])

        # Identificar usuarios nuevos registrados a partir de FECHA_MODO_FULL
        # Se agregan los datos de Usuario y Correo a partir del DataFrame original de usuarios
        usuarios_nuevos_modo = usuarios[usuarios["Fecha_Alta"] >= FECHA_MODO_FULL].copy()

        # Asegurarse de que el DataFrame de nuevos usuarios tenga solo las columnas relevantes
        # Se incluye "Usuario" y "Correo" ahora
        usuarios_nuevos_modo = usuarios_nuevos_modo[["Documento", "Fecha_Alta", "Usuario", "Correo"]].copy()
        
        print("✅ DataFrame generado: usuarios_nuevos_modo")

        # Solo registrados entre 2021 y 2024
        antiguos = usuarios[usuarios["Fecha_Alta"].dt.year.between(2021, 2024)].copy()

        # Cargar movimientos MODO desde el CSV que acabamos de crear
        cargas_modo = pd.read_csv(csv_dir / "movimientos_modo.csv")
        # Convertir explícitamente la columna de fecha para evitar errores
        cargas_modo["Fecha"] = pd.to_datetime(cargas_modo["Fecha"], dayfirst=True, errors="coerce")
        cargas_modo["Documento"] = pd.to_numeric(
            cargas_modo["Documento"].astype(str).str.extract(r"(\d+)")[0].str.lstrip("0"),
            errors="coerce"
        )

        # Quién recargó antes y después del 7/7
        jugaban_antes = set(cargas_modo[cargas_modo["Fecha"] < FECHA_MODO_FULL]["Documento"])
        jugaban_despues = set(cargas_modo[cargas_modo["Fecha"] >= FECHA_MODO_FULL]["Documento"])

        # Reactivados = antiguos que no jugaban antes pero sí después
        reactivados = antiguos[antiguos["Documento"].isin(jugaban_despues - jugaban_antes)].copy()
        
        # Obtener primera recarga MODO desde 7/7
        primer_modo = cargas_modo[cargas_modo["Fecha"] >= FECHA_MODO_FULL].sort_values("Fecha").drop_duplicates("Documento")
        usuarios_reactivados_modo = reactivados.merge(
            primer_modo[["Documento", "Fecha"]],
            left_on="Documento", right_on="Documento", how="left"
        )
        # Intentar eliminar el archivo antes de escribir para evitar "Permission denied"
        reactivados_csv_path = csv_dir / "reactivados_modo.csv"
        if reactivados_csv_path.exists():
            os.remove(reactivados_csv_path)

        usuarios_reactivados_modo.to_csv(reactivados_csv_path, index=False)
        print("✅ CSV generado: reactivados_modo.csv")

        # Exportar el DataFrame de nuevos usuarios directamente al CSV
        nuevos_csv_path = csv_dir / "nuevos_modo.csv"
        if nuevos_csv_path.exists():
            os.remove(nuevos_csv_path)
        usuarios_nuevos_modo.to_csv(nuevos_csv_path, index=False)
        print("✅ CSV generado: nuevos_modo.csv")

        # Generar CSV para el total de nuevos usuarios
        total_nuevos_modo = usuarios_nuevos_modo.shape[0]
        pd.DataFrame([{"KPI": f"Total Nuevos Usuarios desde {FECHA_MODO_FULL.strftime('%d/%m/%Y')}", "Valor": total_nuevos_modo}]).to_csv(csv_dir / "total_usuarios_nuevos_modo.csv", index=False)
        print("✅ CSV generado: total_usuarios_nuevos_modo.csv")


    except Exception as e:
        print(f"⚠️ Error al detectar usuarios reactivados o nuevos por MODO: {e}")

# ========= Top10 por juego con datos personales =========
if USUARIOS_FILE.exists():
    try:
        # Cargar y limpiar usuarios
        usuarios = pd.read_excel(USUARIOS_FILE)
        usuarios.columns = usuarios.columns.str.strip()
        # Normalizar el nombre de la columna "Fecha_Alta" si es necesario
        if "Fecha_Alta" not in usuarios.columns and "Fecha Alta" in usuarios.columns:
            usuarios.rename(columns={"Fecha Alta": "Fecha_Alta"}, inplace=True)
        # Intentar parsear las fechas con el método dayfirst para mayor robustez
        usuarios["Fecha_Alta"] = pd.to_datetime(usuarios["Fecha_Alta"], dayfirst=True, errors="coerce")

        if "Documento" in usuarios.columns:
            usuarios.rename(columns={"Documento": "DNI"}, inplace=True)

        usuarios["DNI"] = pd.to_numeric(usuarios["DNI"], errors="coerce").astype("Int64")
        usuarios = usuarios.dropna(subset=["DNI"])[["DNI", "Usuario", "Correo"]].drop_duplicates("DNI")

        for juego, df in game_summaries.items():
            df = df.copy()
            df["Documento"] = pd.to_numeric(df["Documento"], errors="coerce").astype("Int64")

            merged = df.merge(usuarios, left_on="Documento", right_on="DNI", how="inner")
            top10 = merged.sort_values("Gastado", ascending=False).head(10)

            top10_por_juego_con_datos[juego] = top10

    except Exception as e:
        print(f"⚠️  Error al generar Top10 por juego con datos: {e}")

# Nuevo: generar CSV para el total de apuestas por juego y por mes
total_juegos_mes = (
    apuestas.groupby(["AñoMes", "Juego"])["Importe"]
    .count()
    .rename("Total_Bets")
    .reset_index()
)
total_juegos_mes.to_csv(csv_dir / "total_juegos_mes.csv", index=False)
print("✅ CSV generado: total_juegos_mes.csv")


# ========= Exportar a la plantilla de Excel con xlwings =========
try:
    if not TEMPLATE_PATH.exists():
        print("⚠️ No se encontró el archivo de plantilla. No se puede generar el reporte con macros.")
    else:
        print(f"🔄 Actualizando el reporte de Excel usando la plantilla: {TEMPLATE_PATH.name}…")
        # Abre la plantilla con xlwings (visible=False para que no se muestre)
        wb = xw.Book(TEMPLATE_PATH.name, visible=False)
        
        # Escribe los dataframes procesados en las hojas correspondientes
        # Asegúrate de que los nombres de las hojas y las celdas de inicio coincidan
        wb.sheets["Base_Movimientos"].range("A1").options(index=False).value = data
        wb.sheets["Resumen_Datos"].range("A1").options(index=False).value = resumen_kpis
        wb.sheets["Cliente_Mes"].range("A1").options(index=False).value = cliente_mes
        wb.sheets["Top_Games_Total"].range("A1").options(index=False).value = top_games_total
        wb.sheets["Top_Games_Mes"].range("A1").options(index=False).value = top_games_mes
        wb.sheets["Recargas_Diario"].range("A1").options(index=False).value = recargas_diario_canal
        wb.sheets["MODO_Diario"].range("A1").options(index=False).value = modo_diario
        wb.sheets["Retiros_Diario"].range("A1").options(index=False).value = retiros_diario
        wb.sheets["Ganadores"].range("A1").options(index=False).value = premios_resumen
        wb.sheets["Juego_Dia_Detalle"].range("A1").options(index=False).value = juego_dia_detalle
        wb.sheets["Dia_Totales"].range("A1").options(index=False).value = dia_totales
        wb.sheets["Retencion_MODO"].range("A1").options(index=False).value = retencion_modo
        wb.sheets["Usuarios_Mes"].range("A1").options(index=False).value = usuarios_mes
        wb.sheets["Usuarios_Hitos"].range("A1").options(index=False).value = usuarios_hitos
        wb.sheets["Comparativa_MODO"].range("A1").options(index=False).value = comparativa_modo

        if top10_contactos is not None and not top10_contactos.empty:
             wb.sheets["Top10_Contactos"].range("A1").options(index=False).value = top10_contactos
        
        # Vuelve a calcular las tablas dinámicas y macros si es necesario (con xlwings)
        wb.app.calculate()
        
        # Guarda el archivo con un nuevo nombre
        wb.save(SALIDA_XLWINGS)
        wb.close()
        print(f"✅ Reporte de Excel actualizado y guardado como: {SALIDA_XLWINGS.name}")
        
except Exception as e:
    print(f"⚠️ Error al generar el reporte de Excel con xlwings: {e}")
    print("Por favor, asegúrate de que el archivo de plantilla no esté abierto.")

# ========= Exportar hojas clave como CSV para dashboard HTML =========
def export_csv(sheet_name, filename):
    try:
        df = pd.read_excel(SALIDA_ANALITICO, sheet_name=sheet_name)
        df.to_csv(csv_dir / filename, index=False)
        print(f"✅ CSV generado: {filename}")
    except Exception as e:
        print(f"⚠️ Error al exportar {sheet_name} → {filename}: {e}")

export_csv("MODO_Diario",         "modo_diario.csv")
export_csv("Recargas_Dia_Monto", "recargas_monto.csv")
export_csv("Recargas_Dia_Cant",  "recargas_cant.csv")
export_csv("Comparativa_MODO",   "comparativa_modo.csv")
export_csv("Resumen_Datos",      "kpis.csv")
export_csv("Top_Games_Mes",      "jugadores_unicos_por_juego.csv")

# ========= Exportar deposito_promedio.csv =========
try:
    monto = pd.read_excel(SALIDA_ANALITICO, sheet_name="Recargas_Dia_Monto")
    cant = pd.read_excel(SALIDA_ANALITICO, sheet_name="Recargas_Dia_Cant")
    prom = monto.copy()
    for col in ["MODO", "Retail"]:
        if col in monto.columns and col in cant.columns:
            prom[col] = monto[col] / cant[col]
    prom.to_csv(csv_dir / "deposito_promedio.csv", index=False)
    print("✅ CSV generado: deposito_promedio.csv")
except Exception as e:
    print(f"⚠️ Error al generar deposito_promedio.csv: {e}")

# ========= Exportar reportes adicionales (usuarios) =========
try:
    excel_file = pd.ExcelFile(SALIDA_ANALITICO)

    if "Nuevos_MODO" in excel_file.sheet_names:
        df = excel_file.parse("Nuevos_MODO")
        df.to_csv(csv_dir / "nuevos_modo.csv", index=False)

    if "Reactivados_MODO" in excel_file.sheet_names:
        df = excel_file.parse("Reactivados_MODO")
        df.to_csv(csv_dir / "reactivados_modo.csv", index=False)

    for juego, df in top10_por_juego_con_datos.items():
        archivo = f"top10_{juego.lower().replace(' ', '_')}.csv"
        if not df.empty:
            df.to_csv(csv_dir / archivo, index=False)

    print("✅ CSV adicionales exportados (usuarios únicos, nuevos, reactivados, top10 por juego)")
except Exception as e:
    print(f"⚠️ Error al exportar reportes adicionales: {e}")

print("🏁  Proceso incremental terminado.")