# ------------------------
DETECTAR_OUTLIERS = True     
DETECTAR_ANOMALIAS = False   
# ------------------------

from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np
from pyod.models.iforest import IForest
from datetime import datetime

# Configuración de conexión
DB_CONFIG = {
    "user": "root",
    "password": "1234",
    "host": "127.0.0.1",
    "port": "3306",
    "database": "gp-mlp-telemtry"
}

DATABASE_URL = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
engine = create_engine(DATABASE_URL)

# Función para registrar el log de cada corrida
def registrar_log(row_vigency=0, registers=0, event_count=0, count_alarm=0, start_alarm=None, vigency_alarm=None):
    try:
        now = datetime.now()
        fecha_actual = now.date()
        timestamp_actual = now

        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO log_inserts (
                    row_vigency, registers, date_log, timestamp_log, event_count,
                    count_alarm, start_alarm, vigency_alarm
                ) VALUES (
                    :row_vigency, :registers, :date_log, :timestamp_log, :event_count,
                    :count_alarm, :start_alarm, :vigency_alarm
                )
            """), {
                "row_vigency": row_vigency,
                "registers": registers,
                "date_log": fecha_actual,
                "timestamp_log": timestamp_actual,
                "event_count": event_count,
                "count_alarm": count_alarm,
                "start_alarm": start_alarm if start_alarm else '0000-00-00 00:00:00',
                "vigency_alarm": vigency_alarm if vigency_alarm else '0000-00-00 00:00:00'
            })
        print("[INFO] Log registrado correctamente.")
    except Exception as e:
        print(f"[ERROR] registrar_log: {e}")

# Obtener sensores definidos en la tabla UMBRALES
def obtener_sensores_desde_umbrales():
    try:
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
    except Exception as e:
        print(f"[ERROR] obtener_sensores_desde_umbrales: {e}")
        return pd.DataFrame()

# Obtener datos históricos para un sensor específico
def obtener_datos(estacion_id, sensor_id):
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                CALL `gp-mlp-telemtry`.GetSensorDataIntervalo(:opcion, :estacion_id, :sensor_id, :fecha_inicio, :fecha_fin)
            """), {
                "opcion": 2,
                "estacion_id": estacion_id,
                "sensor_id": sensor_id,
                "fecha_inicio": '2025-03-01 04:30:00',
                "fecha_fin": None
            })
            datos = result.fetchall()
            columnas = result.keys()
        df = pd.DataFrame(datos, columns=columnas)[["fecha_hora", "valor"]]
        df.dropna(subset=["valor"], inplace=True)
        return df
    except Exception as e:
        print(f"[ERROR] obtener_datos: {e}")
        return pd.DataFrame()

# Insertar una alerta en la tabla alertas
def insertar_alerta(sensor_id, estacion_id, fecha_hora, valor, criterio_id=2):
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO alertas (sensor_id, estacion_id, timestamp, valor, criterio_id)
                VALUES (:sensor_id, :estacion_id, :timestamp, :valor, :criterio_id)
            """), {
                "sensor_id": sensor_id,
                "estacion_id": estacion_id,
                "timestamp": fecha_hora,
                "valor": valor,
                "criterio_id": criterio_id
            })
    except Exception as e:
        print(f"[ERROR] insertar_alerta: {e}")

# Detectar valores fuera de los límites configurados
def detectar_outliers_limites(estacion_id, sensor_id, nombre_estacion, datos):
    try:
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
            insertar_alerta(sensor_id, estacion_id, fila['fecha_hora'], fila['valor'], criterio_id=2)

        return datos_outliers
    except Exception as e:
        print(f"[ERROR] detectar_outliers_limites: {e}")
        return pd.DataFrame()

# Detección de anomalías usando Isolation Forest
def detectar_anomalias(datos):
    try:
        if len(datos) < 10:
            return pd.DataFrame()

        X = datos["valor"].values.reshape(-1, 1)

        if np.isnan(X).any():
            return pd.DataFrame()

        modelo = IForest(contamination=0.01)
        modelo.fit(X)
        datos["anomaly"] = modelo.predict(X)
        return datos[datos["anomaly"] == 1]
    except Exception as e:
        print(f"[ERROR] detectar_anomalias: {e}")
        return pd.DataFrame()

# -----------------------------------------------
# BLOQUE PRINCIPAL
# -----------------------------------------------
if __name__ == "__main__":
    try:
        sensores_df = obtener_sensores_desde_umbrales()

        print("Buscando puntos críticos...")
        total_registros = 0
        total_alarmas = 0
        eventos_detectados = 0
        primer_alarma = None

        for _, sensor in sensores_df.iterrows():
            datos = obtener_datos(sensor["estacion_id"], sensor["sensor_id"])
            total_registros += len(datos)

            if not datos.empty:
                if DETECTAR_OUTLIERS:
                    outliers = detectar_outliers_limites(
                        sensor["estacion_id"], 
                        sensor["sensor_id"], 
                        sensor["nombre_estacion"], 
                        datos
                    )
                    eventos_detectados += len(outliers)
                    if not outliers.empty and primer_alarma is None:
                        primer_alarma = outliers.iloc[0]['fecha_hora']
                    total_alarmas += len(outliers)

                if DETECTAR_ANOMALIAS:
                    anomalías = detectar_anomalias(datos)
                    eventos_detectados += len(anomalías)
                    if not anomalías.empty and primer_alarma is None:
                        primer_alarma = anomalías.iloc[0]['fecha_hora']
                    for _, fila in anomalías.iterrows():
                        print(f"[Anomalía-Modelo] Sensor: {sensor['sensor_id']}, Estación: {sensor['nombre_estacion']}, Tipo: {sensor['tipo']}, Fecha: {fila['fecha_hora']}, Valor: {fila['valor']}")

        registrar_log(
            row_vigency=len(sensores_df),
            registers=total_registros,
            event_count=eventos_detectados,
            count_alarm=total_alarmas,
            start_alarm=primer_alarma,
            vigency_alarm=datetime.now()
        )
    except Exception as e:
        print(f"[ERROR] bloque principal: {e}")
