
import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import importlib

# Cargar variables de entorno y crear motor de conexión
load_dotenv()
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

# Parámetros del script
ALGORITMO = "hotelling_test"
FUNCION = "hotelling_T2_univariado"
DIAS_HISTORIA = 30
CARPETA_SALIDA = "resultados_pruebas"

# Crear carpeta si no existe
os.makedirs(CARPETA_SALIDA, exist_ok=True)

# Cargar módulo del algoritmo
algoritmo_modulo = importlib.import_module(ALGORITMO)
algoritmo_funcion = getattr(algoritmo_modulo, FUNCION)

# Función para obtener sensores habilitados
def obtener_sensores_activos():
    query = text("SELECT sensor_id FROM ultimo_dato_sensor WHERE enable = 1")
    with engine.connect() as conn:
        result = conn.execute(query).fetchall()
        return [r[0] for r in result]

# Función para obtener datos históricos de un sensor
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

# Lista para resumen
resumen = []

# Procesar cada sensor
sensores = obtener_sensores_activos()
for sensor_id in sensores:
    try:
        datos = obtener_datos_sensor(sensor_id)
        if datos.empty:
            continue

        resultado = algoritmo_funcion(datos)
        anomalías = resultado[resultado["anomalía"] == True]

        if not anomalías.empty:
            nombre_algoritmo = ALGORITMO.replace("_test", "")
            path = f"{CARPETA_SALIDA}/anomalías_{nombre_algoritmo}_{sensor_id}.csv"
            anomalías.to_csv(path, index=False)

            resumen.append({
                "sensor_id": sensor_id,
                "n_anomalias": len(anomalías),
                "fecha_ultima_anomalia": anomalías["fecha_hora"].max()
            })

    except Exception as e:
        print(f"❌ Error procesando sensor {sensor_id}: {e}")

# Guardar resumen
df_resumen = pd.DataFrame(resumen)
resumen_path = f"{CARPETA_SALIDA}/resumen_anomalías_{ALGORITMO.replace('_test','')}.csv"
df_resumen.to_csv(resumen_path, index=False)

print("✅ Proceso completado. Revisa la carpeta 'resultados_pruebas'")
