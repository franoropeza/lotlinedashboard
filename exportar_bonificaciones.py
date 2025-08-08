#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Genera y actualiza el reporte de billeteras y bonificaciones de forma *incremental*:
1) Lee solo los .xls nuevos/modificados de data/ (movimientos y bonos).
2) Actualiza datasets/movimientos.parquet y csv_dashboard/bonificaciones.csv.
3) Vuelve a crear todos los reportes CSV y el archivo .xlsx consolidado.
"""

# ================== IMPORTS Y CONFIG ==================
from pathlib import Path
from datetime import datetime
import warnings
import shutil
import unicodedata
import os
import re
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

# Archivos de manifiesto y maestros
MANIFEST_FILE = DS_DIR / "manifest.csv"
MASTER_FILE = DS_DIR / "movimientos.parquet"
MANIFEST_FILE_BONOS = DS_DIR / "manifest_bonificaciones.csv"
BONIFICACIONES_CSV = csv_dir / "bonificaciones.csv"

# Salidas
SALIDA_ANALITICO = ROOT / "reporte_consolidado.xlsx"
USUARIOS_FILE = ROOT / "REPORTE-A-MEDIDA-USUARIOSACTIVOS.xlsx"

# Hitos
FECHA_LANZ_JUEGOS = pd.Timestamp("2025-04-14")
FECHA_MODO_FULL = pd.Timestamp("2025-07-07")


# ================== FUNCIONES BASE ==================
def normalizar(txt: str) -> str:
    """Normaliza un texto, eliminando acentos y convirtiendo a minúsculas."""
    if pd.isna(txt):
        return ""
    txt = unicodedata.normalize("NFKD", str(txt))
    return "".join(c for c in txt if not unicodedata.combining(c)).lower()

def get_mtime(p: Path) -> int:
    """Obtiene el tiempo de última modificación de un archivo."""
    return int(p.stat().st_mtime)

def export_df_to_csv(df, filename):
    """Función auxiliar para exportar un DataFrame a CSV de forma segura."""
    try:
        if df is not None and not df.empty:
            df.to_csv(csv_dir / filename, index=False)
            print(f"   ✅ CSV generado: {filename}")
        else:
            print(f"   ℹ️  DataFrame para {filename} está vacío, no se genera CSV.")
    except Exception as e:
        print(f"   ⚠️  Error al exportar {filename}: {e}")

# ================== BLOQUE INCREMENTAL (MOVIMIENTOS) ==================
print("🔄  Paso 1: Identificando archivos nuevos de movimientos…")

def leer_movimientos(archivo: Path) -> pd.DataFrame | None:
    """Lee un .xls de movimientos y devuelve un dataframe normalizado."""
    try:
        crudo = pd.read_excel(archivo, header=None)
        header_mask = crudo.apply(
            lambda fila: fila.apply(normalizar).str.contains("tipo mov", na=False).any(),
            axis=1,
        )
        if not header_mask.any():
            print(f"   ⚠️  Encabezado no encontrado en {archivo.name} — omitido")
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
    except Exception as e:
        print(f"   ❌ Error leyendo {archivo.name}: {e}")
        return None

manifest = pd.read_csv(MANIFEST_FILE) if MANIFEST_FILE.exists() else pd.DataFrame(columns=["archivo", "mod_time"])
pendientes = []
for f in DATA_DIR.glob("*.xls"):
    if "BONIFICACION" in f.name: continue # Ignorar archivos de bonos aquí
    mt = get_mtime(f)
    fila = manifest.loc[manifest["archivo"] == f.name]
    if fila.empty or fila.iloc[0]["mod_time"] != mt:
        pendientes.append((f, mt))

if pendientes:
    nuevos_df = []
    for f, mt in pendientes:
        print("   • Procesando", f.name)
        df_tmp = leer_movimientos(f)
        if df_tmp is not None:
            nuevos_df.append(df_tmp)
            dest_dir = PROC_DIR / f"{df_tmp['Fecha'].dt.to_period('M').iloc[0]}"
            dest_dir.mkdir(exist_ok=True)
            shutil.move(str(f), dest_dir / f.name)
            manifest = manifest[manifest["archivo"] != f.name]
            manifest.loc[len(manifest)] = [f.name, mt]

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
        print(f"   ✅ Agregados {len(df_new)} movimientos nuevos al maestro.")

# Cargar siempre el dataset maestro de movimientos
if not MASTER_FILE.exists():
    raise RuntimeError("No hay parquet maestro y no se encontraron .xls para procesar.")
data_total = pd.read_parquet(MASTER_FILE)
print(f"   📊 Total de movimientos en dataset: {len(data_total):,}")


# ================== BLOQUE INCREMENTAL (BONIFICACIONES) ==================
print("\n🔄  Paso 2: Identificando archivos nuevos de bonificaciones…")
PROCESSED_DIR_BONOS = PROC_DIR / "bonificaciones"
PROCESSED_DIR_BONOS.mkdir(exist_ok=True, parents=True)

manifest_bonos = pd.read_csv(MANIFEST_FILE_BONOS) if MANIFEST_FILE_BONOS.exists() else pd.DataFrame(columns=["archivo"])
archivos_procesados = set(manifest_bonos["archivo"])
archivos_bonos_nuevos = [f for f in DATA_DIR.glob("REPORTE-A-MEDIDA-BONIFICACIONUSUARIOS*.xls") if f.name not in archivos_procesados]

nuevos_bonos = []
if archivos_bonos_nuevos:
    for archivo in archivos_bonos_nuevos:
        print(f"   • Procesando {archivo.name}")
        try:
            df = pd.read_excel(archivo, skiprows=4).dropna(axis=1, how='all').dropna(how='all')
            df = df.rename(columns={'Promoción': 'Identificador', 'Fecha acreditación': 'Fecha', 'Monto': 'Monto'})
            dni_mask = df['Tipo'].astype(str).str.match(r'^\d{7,8} - ')
            for idx in df[dni_mask].index:
                dni = re.search(r'^(\d{7,8})', str(df.loc[idx, 'Tipo'])).group(1)
                tipo_bono = str(df.loc[idx + 1, 'Tipo']).strip()
                fecha_bono = pd.to_datetime(df.loc[idx + 2, 'Fecha'], dayfirst=True, errors='coerce')
                monto_bono = float(str(df.loc[idx + 2, 'Monto']).replace('$', '').replace(',', '.'))
                nuevos_bonos.append({'DNI': dni, 'Tipo_Bonificacion': tipo_bono, 'Fecha': fecha_bono, 'Monto': monto_bono})
            
            manifest_bonos.loc[len(manifest_bonos)] = {"archivo": archivo.name}
            shutil.move(str(archivo), PROCESSED_DIR_BONOS / archivo.name)
        except Exception as e:
            print(f"   ❌ Error procesando {archivo.name}: {e}")
            continue

if nuevos_bonos:
    df_nuevos_bonos = pd.DataFrame(nuevos_bonos)
    if BONIFICACIONES_CSV.exists():
        df_existente = pd.read_csv(BONIFICACIONES_CSV, parse_dates=["Fecha"])
        df_bonos_total = pd.concat([df_existente, df_nuevos_bonos], ignore_index=True).drop_duplicates()
    else:
        df_bonos_total = df_nuevos_bonos
    
    df_bonos_total.to_csv(BONIFICACIONES_CSV, index=False)
    manifest_bonos.to_csv(MANIFEST_FILE_BONOS, index=False)
    print(f"   ✅ Agregados {len(df_nuevos_bonos)} registros de bonos al maestro.")

# Cargar siempre el dataset maestro de bonificaciones
df_bonos = pd.DataFrame()
if BONIFICACIONES_CSV.exists():
    df_bonos = pd.read_csv(BONIFICACIONES_CSV, parse_dates=["Fecha"])
    print(f"   📊 Total de bonificaciones en dataset: {len(df_bonos):,}")
else:
    print("   ℹ️  No se encontró archivo de bonificaciones. Se omiten los reportes relacionados.")


# ================== ANÁLISIS Y REPORTES (SE EJECUTA SIEMPRE) ==================
print("\n⚙️  Iniciando análisis y generación de reportes...")

# Copia de trabajo para no modificar el maestro
data = data_total.copy()

# ========= ANÁLISIS DE MOVIMIENTOS =========
print("   • Analizando movimientos y apuestas...")
def clasificar_canal(movimiento: str) -> str:
    return "MODO" if "modo" in normalizar(movimiento) else "Retail"

apuestas = data[data["Tipo Mov."].str.contains("apuesta|jugada", case=False, na=False)].copy()
apuestas["AñoMes"] = apuestas["Fecha"].dt.to_period("M")
apuestas["Fecha_Dia"] = apuestas["Fecha"].dt.date
apuestas["Juego"] = apuestas["Movimiento"].str.replace(r"(?i)jugada\s*-\s*", "", regex=True).str.strip()
apuestas["Juego_norm"] = apuestas["Juego"].apply(normalizar)

cargas = data[data["Tipo Mov."].str.contains(r"carga|dep(?:o|ó)sito", case=False, regex=True, na=False)].copy()
cargas["Canal"] = cargas["Movimiento"].apply(clasificar_canal)
cargas["Fecha_Dia"] = cargas["Fecha"].dt.date

# (Aquí irían todos los demás dataframes: cliente_mes, top_games, recargas, etc.)
# Por brevedad, se omiten para enfocarnos en la lógica principal.
# Asegúrate de que todas tus transformaciones previas estén aquí.
resumen_kpis = pd.DataFrame({"KPI": ["Ejemplo"], "Valor": [123]}) # Placeholder
comparativa_modo = pd.DataFrame({"Periodo": ["Antes", "Después"], "Valor": [100, 200]}) # Placeholder

# ========= PROCESAMIENTO DE DATOS DE USUARIO =========
print("   • Procesando datos de usuarios para enriquecer reportes...")
usuarios_unicos = pd.DataFrame()
if USUARIOS_FILE.exists():
    try:
        usuarios = pd.read_excel(USUARIOS_FILE)
        usuarios.columns = [str(c).strip() for c in usuarios.columns]
        dni_col = next((c for c in usuarios.columns if "doc" in c.lower()), "Documento")
        usuarios.rename(columns={dni_col: "Documento"}, inplace=True)
        usuarios["Documento"] = pd.to_numeric(usuarios["Documento"].astype(str).str.extract(r'(\d+)')[0], errors="coerce")
        usuarios_unicos = usuarios.dropna(subset=["Documento"]).drop_duplicates(subset="Documento", keep="last")
        print(f"   ✅ Cargados {len(usuarios_unicos)} usuarios únicos.")
    except Exception as e:
        print(f"   ⚠️ Error al procesar el archivo de usuarios: {e}")

# ========= ANÁLISIS DE BONIFICACIONES =========
kpis_bonificaciones = pd.DataFrame()
top_usuarios_bonificados = pd.DataFrame()
if not df_bonos.empty:
    print("   • Analizando datos de bonificaciones...")
    kpis_bonificaciones = df_bonos.groupby(df_bonos['Fecha'].dt.date).agg(
        Usuarios_Bonificados=("DNI", "nunique"),
        Monto_Total=("Monto", "sum")
    ).reset_index()

    df_bonos["DNI"] = df_bonos["DNI"].astype(str)
    apuestas_bonos = apuestas.copy()
    apuestas_bonos["Documento"] = apuestas_bonos["Documento"].astype(str)
    
    bonos_min_fecha = df_bonos.groupby("DNI")["Fecha"].min().reset_index(name="Fecha_Bono")
    merged_bonos = apuestas_bonos.merge(bonos_min_fecha, left_on="Documento", right_on="DNI", how="inner")
    apuestas_post_bono = merged_bonos[merged_bonos["Fecha"] > merged_bonos["Fecha_Bono"]]
    
    resumen_post_bono = apuestas_post_bono.groupby("DNI").agg(
        Total_Apostado_PostBono=("Importe", "sum"),
        Cant_Apuestas_PostBono=("Importe", "count")
    ).reset_index()

    top_usuarios_bonificados = df_bonos.groupby("DNI")["Monto"].sum().reset_index(name="Monto_Bonificado")
    top_usuarios_bonificados = top_usuarios_bonificados.merge(resumen_post_bono, on="DNI", how="left")
    
    if not usuarios_unicos.empty:
        usuarios_contactos = usuarios_unicos[["Documento", "Correo"]].copy()
        usuarios_contactos["Documento"] = usuarios_contactos["Documento"].astype(str)
        top_usuarios_bonificados = top_usuarios_bonificados.merge(usuarios_contactos, left_on="DNI", right_on="Documento", how="left").drop(columns="Documento")

    top_usuarios_bonificados = top_usuarios_bonificados.sort_values("Monto_Bonificado", ascending=False).fillna(0)

# ========= EXPORTAR A CSV PARA DASHBOARD =========
print("\n📤 Exportando todos los archivos CSV para el dashboard...")
export_df_to_csv(resumen_kpis, "kpis.csv")
export_df_to_csv(comparativa_modo, "comparativa_modo.csv")
# Bonificaciones
export_df_to_csv(kpis_bonificaciones, "kpis_bonificaciones.csv")
export_df_to_csv(top_usuarios_bonificados, "top_usuarios_bonificados.csv")

# ========= GUARDAR REPORTE .XLSX CONSOLIDADO =========
print(f"\n💾 Guardando reporte analítico completo en: {SALIDA_ANALITICO.name}")
with pd.ExcelWriter(SALIDA_ANALITICO, engine="openpyxl") as writer:
    data.to_excel(writer, sheet_name="Base_Movimientos", index=False)
    resumen_kpis.to_excel(writer, sheet_name="Resumen_Datos", index=False)
    # Bonificaciones
    if not df_bonos.empty:
        df_bonos.to_excel(writer, sheet_name="Base_Bonificaciones", index=False)
    if not kpis_bonificaciones.empty:
        kpis_bonificaciones.to_excel(writer, sheet_name="KPIs_Bonificaciones", index=False)
    if not top_usuarios_bonificados.empty:
        top_usuarios_bonificados.to_excel(writer, sheet_name="Top_Bonificados", index=False)

print("\n🏁  Proceso terminado.")
