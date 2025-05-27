import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import importlib
import inspect

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
    "rolling_zscore",
    "rolling_mad"
]
PARAMETROS_PERSONALIZADOS = {
    "rolling_zscore": {"window": 48, "threshold": 4.0},
    "rolling_mad": {"window": 48, "threshold": 5.0, "min_mad": 0.05}
}
DIAS_HISTORIA = 30
CARPETA_SALIDA = "resultados_pruebas"
CARPETA_RAW = os.path.join(CARPETA_SALIDA, "raw")
os.makedirs(CARPETA_SALIDA, exist_ok=True)
os.makedirs(CARPETA_RAW, exist_ok=True)

# Lista de tipos_raw excluidos para análisis predictivo
TIPOS_RAW_EXCLUIDOS = ["Caudal", "Caudal de extraccion", "Nivel", "Nivel freático"]

# Importar módulo de algoritmos
algoritmos = importlib.import_module(MODULO_ALGORITMOS)

def obtener_sensores_activos():
    query = text("""
        SELECT s.sensor_id
        FROM Sensores s
        JOIN ultimo_dato_sensor uds ON s.sensor_id = uds.sensor_id
        WHERE uds.enable = 1
        AND s.tipo_raw NOT IN ('Caudal', 'Caudal de extraccion', 'Nivel', 'Nivel freático')
    """)
    with engine.connect() as conn:
        result = conn.execute(query).fetchall()
        return [r[0] for r in result]

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

def obtener_tipo_raw(sensor_id):
    query = text("SELECT tipo_raw FROM Sensores WHERE sensor_id = :sensor_id")
    with engine.connect() as conn:
        result = conn.execute(query, {"sensor_id": sensor_id}).fetchone()
        return result[0] if result else None

# Ejecutar pruebas
resumen_general = []
sensores = obtener_sensores_activos()
print(f"Procesando {len(sensores)} sensores con {len(FUNCIONES_A_PROBAR)} algoritmos...")

for sensor_id in sensores:
    datos = obtener_datos_sensor(sensor_id)
    if datos.empty:
        continue

    datos.to_csv(os.path.join(CARPETA_RAW, f"datos_raw_{sensor_id}.csv"), index=False)
    tipo_raw = obtener_tipo_raw(sensor_id)

    if tipo_raw in TIPOS_RAW_EXCLUIDOS:
        print(f"🔕 Sensor {sensor_id} (tipo_raw = '{tipo_raw}') excluido del análisis predictivo.")
        continue

    for funcion in FUNCIONES_A_PROBAR:
        try:
            algoritmo = getattr(algoritmos, funcion)
            parametros = PARAMETROS_PERSONALIZADOS.get(funcion, {})

            # Inspeccionar si acepta argumentos extra
            args = inspect.signature(algoritmo).parameters
            if "df" in args:
                resultado = algoritmo(datos, **parametros)
            else:
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

# Guardar resumen general
df_resumen = pd.DataFrame(resumen_general)
df_resumen.to_csv(f"{CARPETA_SALIDA}/resumen_anomalias_todos.csv", index=False)
print("✅ Análisis completado. Resultados guardados.")
