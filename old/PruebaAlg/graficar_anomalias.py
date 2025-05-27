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

# Configuración de carpetas
CARPETA_SALIDA = "resultados_pruebas"
CARPETA_RAW = os.path.join(CARPETA_SALIDA, "raw")
CARPETA_GRAFICOS = os.path.join(CARPETA_SALIDA, "graficos")
os.makedirs(CARPETA_GRAFICOS, exist_ok=True)

# Estilos
# Cada algoritmo tendrá un color y marcador distinto
marcadores = {
    "rolling_zscore": 'o',
    "isolation_forest": 's',
    "hotelling_T2_univariado": '^'
}
colores = {
    "rolling_zscore": 'red',
    "isolation_forest": 'blue',
    "hotelling_T2_univariado": 'green'
}

# Obtener tipo_raw desde la BD
def obtener_tipo_raw(sensor_id):
    query = text("SELECT tipo_raw FROM Sensores WHERE sensor_id = :sensor_id")
    with engine.connect() as conn:
        result = conn.execute(query, {"sensor_id": int(sensor_id)}).fetchone()
        return result[0] if result else "Desconocido"

# Detectar sensores desde los archivos RAW
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

# Algoritmos que se desean graficar
algoritmos_graficar = [
    ("rolling_zscore", "Rolling Z-Score"),
    ("isolation_forest", "Isolation Forest"),
    ("hotelling_T2_univariado", "Hotelling T² Univariado")
]

# Graficar por sensor
for sensor_id in sensor_ids:
    try:
        raw_path = os.path.join(CARPETA_RAW, f"datos_raw_{sensor_id}.csv")
        df = pd.read_csv(raw_path, parse_dates=["fecha_hora"])
        tipo_raw = obtener_tipo_raw(sensor_id)
        archivos_alg = anomalias_por_sensor.get(sensor_id, {})

        # Crear figura con 3 subplots (uno por algoritmo)
        fig, axs = plt.subplots(3, 1, figsize=(14, 12), sharex=True)

        for i, (algoritmo, titulo) in enumerate(algoritmos_graficar):
            axs[i].plot(df["fecha_hora"], df["valor"], label="Valor original", color="black", linewidth=1)
            if algoritmo in archivos_alg:
                path = os.path.join(CARPETA_SALIDA, archivos_alg[algoritmo])
                df_anom = pd.read_csv(path, parse_dates=["fecha_hora"])
                axs[i].scatter(
                    df_anom["fecha_hora"],
                    df_anom["valor"],
                    label=algoritmo,
                    s=60,
                    marker=marcadores[algoritmo],
                    color=colores[algoritmo],
                    edgecolors='black'
                )
            axs[i].set_title(f"Sensor {sensor_id} - {tipo_raw} - {titulo}")
            axs[i].set_ylabel("Valor")
            axs[i].legend()
            axs[i].grid(True)

        axs[2].set_xlabel("Fecha")
        axs[2].tick_params(axis='x', rotation=45)

        # Guardar imagen
        output_path = os.path.join(CARPETA_GRAFICOS, f"triple_grafico_sensor_{sensor_id}.png")
        plt.tight_layout()
        plt.savefig(output_path)
        plt.close()
        print(f"✅ Gráfico guardado: {output_path}")

    except Exception as e:
        print(f"❌ Error al graficar sensor {sensor_id}: {e}")
