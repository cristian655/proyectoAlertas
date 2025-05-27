import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import importlib

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
MODULO_ALGORITMOS = "algoritmos_univariados"
FUNCIONES_A_PROBAR = [
    "hotelling_T2_univariado",
    "isolation_forest",
    "rolling_zscore"#,
    #"rolling_mad",
    #"one_class_svm",
    #"pca_hotelling"
]
DIAS_HISTORIA = 60
CARPETA_SALIDA = "resultados_pruebas"
CARPETA_RAW = os.path.join(CARPETA_SALIDA, "raw")

# Crear carpetas
os.makedirs(CARPETA_SALIDA, exist_ok=True)
os.makedirs(CARPETA_RAW, exist_ok=True)

# Importar módulo de algoritmos
algoritmos = importlib.import_module(MODULO_ALGORITMOS)

# Obtener sensores activos
def obtener_sensores_activos():
    query = text("SELECT sensor_id FROM ultimo_dato_sensor WHERE enable = 1")
    with engine.connect() as conn:
        result = conn.execute(query).fetchall()
        return [r[0] for r in result]

# Obtener datos de sensor
def obtener_datos_sensor(sensor_id, dias=DIAS_HISTORIA):
    query = text("""
        SELECT fecha_hora, valor 
        FROM Datos
        WHERE sensor_id = :sensor_id AND fecha_hora >= NOW() - INTERVAL :dias DAY
        ORDER BY fecha_hora
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {"sensor_id": sensor_id, "dias": dias})
        return pd.DataFrame(result.fetchall(), columns=["fecha_hora", "valor"])

# Ejecutar pruebas
resumen_general = []

sensores = obtener_sensores_activos()
print(f"Procesando {len(sensores)} sensores con {len(FUNCIONES_A_PROBAR)} algoritmos...")

for sensor_id in sensores:
    datos = obtener_datos_sensor(sensor_id)
    if datos.empty:
        continue

    # Guardar datos RAW por sensor
    datos.to_csv(os.path.join(CARPETA_RAW, f"datos_raw_{sensor_id}.csv"), index=False)

    for funcion in FUNCIONES_A_PROBAR:
        try:
            algoritmo = getattr(algoritmos, funcion)
            resultado = algoritmo(datos)

            if "anomalía" in resultado.columns:
                anomalías = resultado[resultado["anomalía"] == True]
                if not anomalías.empty:
                    path = f"{CARPETA_SALIDA}/{sensor_id}_anomalías_{funcion}.csv"
                    anomalías.to_csv(path, index=False)

                    resumen_general.append({
                        "sensor_id": sensor_id,
                        "algoritmo": funcion,
                        "n_anomalias": len(anomalías),
                        "fecha_ultima_anomalia": anomalías["fecha_hora"].max()
                    })
        except Exception as e:
            print(f"❌ Error en {funcion} con sensor {sensor_id}: {e}")

# Guardar resumen
df_resumen = pd.DataFrame(resumen_general)
df_resumen.to_csv(f"{CARPETA_SALIDA}/resumen_anomalias_todos.csv", index=False)
print("✅ Pruebas completadas. Resultados guardados.")
