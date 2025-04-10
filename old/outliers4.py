# ------------------------
DETECTAR_OUTLIERS = True     
DETECTAR_ANOMALIAS = False   
# ------------------------

from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np
from pyod.models.iforest import IForest

# Configuración de conexión
DB_CONFIG = {
    "user": "master_admin",
    "password": "LtH40j907uN0hwPm9Xoo",
    "host": "db-gp-mlp-dev-01.cn24ekkaknsg.us-east-1.rds.amazonaws.com",
    "port": "3306",
    "database": "GP-MLP-Telemtry"
}

DATABASE_URL = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
engine = create_engine(DATABASE_URL)

# Obtener sensores definidos en la tabla UMBRALES (ya filtrados por estado y activación de umbral)
def obtener_sensores_desde_umbrales():
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT 
                u.sensor_id,
                u.estacion_id,
                s.tipo,
                e.nombre AS nombre_estacion
            FROM umbrales u
            INNER JOIN Sensores s ON u.sensor_id = s.sensor_id
            INNER JOIN Estaciones e ON u.estacion_id = e.estacion_id
            WHERE u.tiene_umbrales = TRUE AND u.enable = 1
        """))
        datos = result.fetchall()
        columnas = result.keys()
    return pd.DataFrame(datos, columns=columnas)

# Obtener datos históricos para un sensor específico
def obtener_datos(estacion_id, sensor_id):
    with engine.connect() as conn:
        result = conn.execute(text("""
            CALL `GP-MLP-Telemtry`.GetSensorData(:estacion, :sensor, :periodo)
        """), {
            "estacion": estacion_id,
            "sensor": sensor_id,
            "periodo": 3
        })
        datos = result.fetchall()
        columnas = result.keys()
    df = pd.DataFrame(datos, columns=columnas)[["fecha_hora", "valor"]]
    df.dropna(subset=["valor"], inplace=True)
    return df

# Detectar valores fuera de los límites configurados
def detectar_outliers_limites(estacion_id, sensor_id, nombre_estacion, datos):
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT 
                u.umbral_minimo AS limite_inferior, 
                u.umbral_maximo AS limite_superior,
                s.tipo
            FROM umbrales u
            INNER JOIN Sensores s ON u.sensor_id = s.sensor_id
            WHERE u.sensor_id = :sensor_id 
              AND u.estacion_id = :estacion_id 
              AND u.tiene_umbrales = TRUE 
              AND u.enable = 1
        """), {"sensor_id": sensor_id, "estacion_id": estacion_id}).mappings().fetchone()

    if not result:
        print(f"[Advertencia] Sensor {sensor_id} no tiene límites válidos.")
        return pd.DataFrame()

    limite_inf = result["limite_inferior"]
    limite_sup = result["limite_superior"]
    tipo = result["tipo"]

    if limite_inf is not None and limite_sup is not None:
        datos_outliers = datos[(datos["valor"] < limite_inf) | (datos["valor"] > limite_sup)]
    elif limite_inf is not None:
        datos_outliers = datos[datos["valor"] < limite_inf]
    elif limite_sup is not None:
        datos_outliers = datos[datos["valor"] > limite_sup]
    else:
        print(f"[Advertencia] Sensor {sensor_id} no tiene ningún límite definido.")
        return pd.DataFrame()

    for _, fila in datos_outliers.iterrows():
        print(f"[Outlier-Límites] Sensor: {sensor_id}, Estación: {nombre_estacion}, Tipo: {tipo}, Fecha: {fila['fecha_hora']}, Valor: {fila['valor']}, Límites: [{limite_inf}, {limite_sup}]")

    return datos_outliers

# Detección de anomalías usando Isolation Forest (si se activa)
def detectar_anomalias(datos):
    if len(datos) < 10:
        return pd.DataFrame()

    X = datos["valor"].values.reshape(-1, 1)

    if np.isnan(X).any():
        return pd.DataFrame()

    modelo = IForest(contamination=0.01)
    modelo.fit(X)
    datos["anomaly"] = modelo.predict(X)
    return datos[datos["anomaly"] == 1]

# -----------------------------------------------
# BLOQUE PRINCIPAL
# -----------------------------------------------
if __name__ == "__main__":
    sensores_df = obtener_sensores_desde_umbrales()

    print("Buscando puntos críticos...")
    for _, sensor in sensores_df.iterrows():
        datos = obtener_datos(sensor["estacion_id"], sensor["sensor_id"])
        if not datos.empty:
            if DETECTAR_OUTLIERS:
                detectar_outliers_limites(
                    sensor["estacion_id"], 
                    sensor["sensor_id"], 
                    sensor["nombre_estacion"], 
                    datos
                )

            if DETECTAR_ANOMALIAS:
                anomalías = detectar_anomalias(datos)
                if not anomalías.empty:
                    for _, fila in anomalías.iterrows():
                        print(f"[Anomalía-Modelo] Sensor: {sensor['sensor_id']}, Estación: {sensor['nombre_estacion']}, Tipo: {sensor['tipo']}, Fecha: {fila['fecha_hora']}, Valor: {fila['valor']}")
