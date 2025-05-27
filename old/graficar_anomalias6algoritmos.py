import os
import pandas as pd
import matplotlib.pyplot as plt
import re
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

# Configuración
CARPETA_SALIDA = "resultados_pruebas"
CARPETA_RAW = os.path.join(CARPETA_SALIDA, "raw")
CARPETA_GRAFICOS = os.path.join(CARPETA_SALIDA, "graficos")
os.makedirs(CARPETA_GRAFICOS, exist_ok=True)

# Estilos
marcadores = ['o', 's', '^', 'v', 'D', 'P', '*', 'X', '<', '>']
colores = ['red', 'blue', 'green', 'orange', 'purple', 'brown', 'magenta', 'cyan', 'gray', 'olive']

# Obtener tipo_raw desde la BD
def obtener_tipo_raw(sensor_id):
    query = text("SELECT tipo_raw FROM Sensores WHERE sensor_id = :sensor_id")
    with engine.connect() as conn:
        result = conn.execute(query, {"sensor_id": int(sensor_id)}).fetchone()
        return result[0] if result else "Desconocido"

# Detectar sensores
sensor_ids = []
for f in os.listdir(CARPETA_RAW):
    match = re.search(r"datos_raw_(\d+).csv", f)
    if match:
        sensor_ids.append(match.group(1))

# Indexar archivos de anomalías
anomalias_por_sensor = {}
anomaly_files = [f for f in os.listdir(CARPETA_SALIDA) if f.endswith(".csv") and "anomalías" in f]
for file in anomaly_files:
    match = re.search(r"(\d+).*anomalías_([a-zA-Z0-9_]+)", file) or re.search(r"anomalías_([a-zA-Z0-9_]+)_(\d+)", file)
    if match:
        if file.startswith("anomalías_"):
            algoritmo, sid = match.group(1), match.group(2)
        else:
            sid, algoritmo = match.group(1), match.group(2)
        anomalias_por_sensor.setdefault(sid, {})[algoritmo] = file

# Graficar
for sensor_id in sensor_ids:
    try:
        raw_path = os.path.join(CARPETA_RAW, f"datos_raw_{sensor_id}.csv")
        df = pd.read_csv(raw_path, parse_dates=["fecha_hora"])
        tipo_raw = obtener_tipo_raw(sensor_id)
        archivos_alg = anomalias_por_sensor.get(sensor_id, {})

        fig, axs = plt.subplots(3, 1, figsize=(14, 12), sharex=True)

        # --- 1. Rolling MAD ---
        axs[0].plot(df["fecha_hora"], df["valor"], label="Valor original", color="black", linewidth=1)
        if "rolling_mad" in archivos_alg:
            path = os.path.join(CARPETA_SALIDA, archivos_alg["rolling_mad"])
            df_anom = pd.read_csv(path, parse_dates=["fecha_hora"])
            axs[0].scatter(df_anom["fecha_hora"], df_anom["valor"], label="rolling_mad", s=60, marker='o',
                           color='red', edgecolors='black')
        axs[0].set_title(f"Sensor {sensor_id} - {tipo_raw} - Rolling MAD")
        axs[0].set_ylabel("Valor")
        axs[0].legend()
        axs[0].grid(True)

        # --- 2. Isolation Forest ---
        axs[1].plot(df["fecha_hora"], df["valor"], label="Valor original", color="black", linewidth=1)
        if "isolation_forest" in archivos_alg:
            path = os.path.join(CARPETA_SALIDA, archivos_alg["isolation_forest"])
            df_anom = pd.read_csv(path, parse_dates=["fecha_hora"])
            axs[1].scatter(df_anom["fecha_hora"], df_anom["valor"], label="isolation_forest", s=60, marker='s',
                           color='blue', edgecolors='black')
        axs[1].set_title("Isolation Forest")
        axs[1].set_ylabel("Valor")
        axs[1].legend()
        axs[1].grid(True)

        # --- 3. Otros algoritmos ---
        axs[2].plot(df["fecha_hora"], df["valor"], label="Valor original", color="black", linewidth=1)
        idx_offset = 0
        for algoritmo, archivo_csv in sorted(archivos_alg.items()):
            if algoritmo in ["rolling_mad", "isolation_forest"]:
                continue
            path = os.path.join(CARPETA_SALIDA, archivo_csv)
            df_anom = pd.read_csv(path, parse_dates=["fecha_hora"])
            if not df_anom.empty and "valor" in df_anom.columns:
                axs[2].scatter(
                    df_anom["fecha_hora"],
                    df_anom["valor"],
                    label=algoritmo,
                    s=50,
                    marker=marcadores[idx_offset % len(marcadores)],
                    color=colores[idx_offset % len(colores)],
                    edgecolors='black'
                )
                idx_offset += 1
        axs[2].set_title("Otros algoritmos")
        axs[2].set_xlabel("Fecha")
        axs[2].set_ylabel("Valor")
        axs[2].legend()
        axs[2].grid(True)
        axs[2].tick_params(axis='x', rotation=45)

        # Guardar gráfico
        plt.tight_layout()
        plt.savefig(os.path.join(CARPETA_GRAFICOS, f"triple_grafico_sensor_{sensor_id}.png"))
        plt.close()

    except Exception as e:
        print(f"❌ Error al graficar sensor {sensor_id}: {e}")
