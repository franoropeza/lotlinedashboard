# -*- coding: utf-8 -*-
"""
Este script identifica a los usuarios registrados que no tienen ningún
movimiento en la plataforma, para poder contactarlos u ofrecerles un bono.
"""
import pandas as pd
from pathlib import Path
import sys

# ========= CONFIGURACIÓN DE ARCHIVOS =========
# Asegúrate de que esta configuración apunte a los mismos archivos que tu script principal
ROOT = Path(__file__).parent
USUARIOS_FILE = ROOT / "REPORTE-A-MEDIDA-USUARIOSACTIVOS.xlsx"
MASTER_FILE = ROOT / "datasets" / "movimientos.parquet"
SALIDA_FILE = ROOT / "usuarios_inactivos_para_bono.xlsx"

# ========= PASO 1: Cargar la lista de TODOS los usuarios registrados =========
print("🔄  Paso 1: Cargando la lista completa de usuarios registrados...")

if not USUARIOS_FILE.exists():
    print(f"❌ ERROR: No se encontró el archivo de usuarios: {USUARIOS_FILE}")
    sys.exit() # Termina el script si el archivo no existe

try:
    # Cargar el archivo de usuarios y limpiar los nombres de las columnas
    df_all_users = pd.read_excel(USUARIOS_FILE)
    df_all_users.columns = df_all_users.columns.str.strip().str.replace(" ", "_")
    
    # Estandarizar la columna de Documento/DNI (lógica similar a tu script)
    if "Documento" in df_all_users.columns:
        user_id_col = "Documento"
    elif "DNI" in df_all_users.columns:
        user_id_col = "DNI"
    else:
        # Busca una columna candidata si los nombres exactos no existen
        cand = [c for c in df_all_users.columns if "doc" in c.lower() or "dni" in c.lower()]
        if not cand:
            raise KeyError("No se encontró una columna de DNI/Documento en el archivo de usuarios.")
        user_id_col = cand[0]
        
    # Limpiar y convertir la columna de ID a numérico para una comparación segura
    df_all_users.rename(columns={user_id_col: "Documento"}, inplace=True)
    df_all_users["Documento"] = pd.to_numeric(df_all_users["Documento"], errors='coerce')
    df_all_users.dropna(subset=["Documento"], inplace=True)
    df_all_users["Documento"] = df_all_users["Documento"].astype(int)
    
    print(f"✅ Encontrados {len(df_all_users)} usuarios registrados en total.")

except Exception as e:
    print(f"❌ ERROR al leer el archivo de usuarios: {e}")
    sys.exit()


# ========= PASO 2: Cargar el historial de movimientos =========
print("\n🔄  Paso 2: Cargando el historial de movimientos para identificar usuarios activos...")

if not MASTER_FILE.exists():
    print(f"❌ ERROR: No se encontró el archivo maestro de movimientos: {MASTER_FILE}")
    print("Asegúrate de ejecutar el script 'generar_reporte_incremental.py' al menos una vez.")
    sys.exit()

try:
    df_movements = pd.read_parquet(MASTER_FILE)
    # Obtener una lista única de los documentos de usuarios que tienen movimientos
    active_user_ids = df_movements["Documento"].unique()
    
    print(f"✅ Encontrados {len(active_user_ids)} usuarios con al menos un movimiento.")
    
except Exception as e:
    print(f"❌ ERROR al leer el archivo de movimientos: {e}")
    sys.exit()


# ========= PASO 3: Identificar a los usuarios inactivos =========
print("\n🔄  Paso 3: Comparando las listas para encontrar usuarios inactivos...")

# Usamos isin() para crear una máscara booleana.
# El símbolo '~' invierte la máscara para seleccionar a los que NO están en la lista de activos.
mask_inactive = ~df_all_users["Documento"].isin(active_user_ids)
df_inactive_users = df_all_users[mask_inactive]

print(f"✅ ¡Encontrados {len(df_inactive_users)} usuarios inactivos!")


# ========= PASO 4: Guardar el resultado en un archivo Excel =========
print(f"\n🔄  Paso 4: Guardando el listado en un archivo Excel...")

try:
    df_inactive_users.to_excel(SALIDA_FILE, index=False)
    print(f"\n🎉 ¡Proceso terminado! El archivo ha sido guardado en:")
    print(f"{SALIDA_FILE.resolve()}")
except Exception as e:
    print(f"❌ ERROR al guardar el archivo de salida: {e}")