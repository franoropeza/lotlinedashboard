import pandas as pd
import re
from pathlib import Path
import shutil

# Rutas de entrada y salida
DATA_DIR = Path("data")
PROCESSED_DIR = DATA_DIR / "processed" / "bonificaciones"
CSV_DIR = Path("csv_dashboard")
DS_DIR = Path("datasets")
USUARIOS_FILE = Path("REPORTE-A-MEDIDA-USUARIOSACTIVOS.xlsx")
MOVIMIENTOS_PARQUET = DS_DIR / "movimientos.parquet"
MANIFEST_FILE = DS_DIR / "manifest_bonificaciones.csv"

CSV_DIR.mkdir(exist_ok=True, parents=True)
DS_DIR.mkdir(exist_ok=True, parents=True)
PROCESSED_DIR.mkdir(exist_ok=True, parents=True)

# Cargar manifest
if MANIFEST_FILE.exists():
    manifest = pd.read_csv(MANIFEST_FILE)
    archivos_procesados = set(manifest["archivo"])
else:
    manifest = pd.DataFrame(columns=["archivo"])
    archivos_procesados = set()

# Buscar archivos nuevos
archivos = list(DATA_DIR.glob("REPORTE-A-MEDIDA-BONIFICACIONUSUARIOS*.xls"))
nuevos = [f for f in archivos if f.name not in archivos_procesados]

bonos_total = []

# Procesar cada nuevo archivo
for archivo in nuevos:
    try:
        df = pd.read_excel(archivo, skiprows=4)
        df = df.dropna(axis=1, how='all').dropna(how='all')
        df = df.rename(columns={
            'Promoción': 'Identificador',
            'Fecha acreditación': 'Fecha',
            'Monto': 'Monto'
        })

        dni_mask = df['Tipo'].astype(str).str.match(r'^\d{7,8} - ')
        dni_indices = df[dni_mask].index
        registros = []

        for idx in dni_indices:
            try:
                fila_dni = df.loc[idx]
                fila_tipo = df.loc[idx + 1]
                fila_monto = df.loc[idx + 2]

                dni = re.search(r'^(\d{7,8})', str(fila_dni['Tipo'])).group(1)
                tipo_bonificacion = str(fila_tipo['Tipo']).strip()
                fecha = pd.to_datetime(fila_monto['Fecha'], dayfirst=True, errors='coerce')
                monto_raw = str(fila_monto['Monto']).replace('$', '').replace(',', '.')
                monto = float(monto_raw)

                registros.append({
                    'DNI': dni,
                    'Tipo_Bonificacion': tipo_bonificacion,
                    'Fecha': fecha,
                    'Monto': monto
                })

            except Exception:
                continue

        if registros:
            bonos_total.extend(registros)
            manifest.loc[len(manifest)] = {"archivo": archivo.name}
            shutil.move(str(archivo), PROCESSED_DIR / archivo.name)
    except Exception:
        continue

# Si hay nuevos, actualizar CSVs
if bonos_total:
    df_bonos = pd.DataFrame(bonos_total)

    # Cargar CSV previo si existe y combinar
    output_file = CSV_DIR / "bonificaciones.csv"
    if output_file.exists():
        df_existente = pd.read_csv(output_file, parse_dates=["Fecha"])
        df_bonos = pd.concat([df_existente, df_bonos], ignore_index=True).drop_duplicates()

    df_bonos.to_csv(output_file, index=False)

    # Crear KPIs por fecha
    kpis_fecha = (
        df_bonos.groupby("Fecha")
                .agg(Usuarios_Bonificados=("DNI", "nunique"), Monto_Total=("Monto", "sum"))
                .reset_index()
    )
    kpis_fecha.to_csv(CSV_DIR / "kpis_bonificaciones.csv", index=False)

    # Cargar movimientos si existe
    if MOVIMIENTOS_PARQUET.exists():
        df_mov = pd.read_parquet(MOVIMIENTOS_PARQUET)
        df_mov["Fecha"] = pd.to_datetime(df_mov["Fecha"], dayfirst=True, errors="coerce")
        df_mov["Documento"] = df_mov["Documento"].astype(str)

        bonos_min_fecha = df_bonos.copy()
        bonos_min_fecha["DNI"] = bonos_min_fecha["DNI"].astype(str)
        bonos_min_fecha = bonos_min_fecha.groupby("DNI")["Fecha"].min().reset_index(name="Fecha_Bono")
        df_mov = df_mov[df_mov["Tipo Mov."].str.contains("apuesta|jugada", case=False, na=False)]

        # Cruce: apuestas después del bono
        apuestas = df_mov.merge(bonos_min_fecha, left_on="Documento", right_on="DNI", how="inner")
        apuestas_post = apuestas[apuestas["Fecha"] > apuestas["Fecha_Bono"]]

        resumen_post = (
            apuestas_post.groupby("DNI")
                         .agg(Total_Apostado_PostBono=("Importe", "sum"),
                              Cant_Apuestas_PostBono=("Importe", "count"))
                         .reset_index()
        )
    else:
        resumen_post = pd.DataFrame(columns=["DNI", "Total_Apostado_PostBono", "Cant_Apuestas_PostBono"])

    # Cargar emails si existen
    if USUARIOS_FILE.exists():
        df_usuarios = pd.read_excel(USUARIOS_FILE)
        df_usuarios.columns = df_usuarios.columns.str.strip()
        if "Documento" not in df_usuarios.columns and "DNI" in df_usuarios.columns:
            df_usuarios.rename(columns={"DNI": "Documento"}, inplace=True)
        df_usuarios["Documento"] = pd.to_numeric(df_usuarios["Documento"], errors="coerce").astype("Int64")
        df_usuarios = df_usuarios.dropna(subset=["Documento", "Correo"])

        df_emails = df_usuarios[["Documento", "Correo"]].drop_duplicates().rename(columns={"Documento": "DNI"})
        df_emails["DNI"] = df_emails["DNI"].astype(str)
    else:
        df_emails = pd.DataFrame(columns=["DNI", "Correo"])

    df_bonos["DNI"] = df_bonos["DNI"].astype(str)

    top_usuarios = (
        df_bonos.groupby("DNI")["Monto"]
                .sum()
                .reset_index(name="Monto_Bonificado")
                .merge(resumen_post, on="DNI", how="left")
                .merge(df_emails, on="DNI", how="left")
                .sort_values("Monto_Bonificado", ascending=False)
    )
    top_usuarios.to_csv(CSV_DIR / "top_usuarios_bonificados.csv", index=False)

    manifest.to_csv(MANIFEST_FILE, index=False)
    print(f"✅ Bonificaciones procesadas: {len(df_bonos)} registros totales, {top_usuarios['DNI'].nunique()} usuarios únicos.")
else:
    print("⚠️ No se detectaron archivos nuevos o válidos para procesar.")
