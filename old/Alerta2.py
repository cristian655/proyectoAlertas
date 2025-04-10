from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np
from pyod.models.iforest import IForest
from datetime import datetime

# ------------------------
DETECTAR_OUTLIERS = True     
DETECTAR_ANOMALIAS = False   
VALIDAR_GET_SENSOR_DETENIDO = False
# ------------------------

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

def obtener_ultima_lectura(sensor_id, estacion_id):
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT max_timestamp AS fecha_hora
                FROM ultimo_dato_sensor
                WHERE sensor_id = :sensor_id AND estacion_id = :estacion_id
            """), {
                "sensor_id": sensor_id,
                "estacion_id": estacion_id
            }).mappings().fetchone()

            if not result:
                return pd.DataFrame()

            fecha_hora = result["fecha_hora"]

            valor_result = conn.execute(text("""
                SELECT valor
                FROM `Datos`
                WHERE sensor_id = :sensor_id AND timestamp_arrive = :timestamp
                ORDER BY timestamp_arrive DESC LIMIT 1
            """), {
                "sensor_id": sensor_id,
                "timestamp": fecha_hora
            }).mappings().fetchone()

            if not valor_result:
                return pd.DataFrame()

            return pd.DataFrame([{
                "fecha_hora": fecha_hora,
                "valor": valor_result["valor"]
            }])
    except Exception as e:
        print(f"[ERROR] obtener_ultima_lectura: {e}")
        return pd.DataFrame()

def registrar_alarma_persistente(sensor_id, estacion_id, fecha_hora, valor, criterio_id=2, observacion=None):
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                SELECT alerta_id, timestamp 
                FROM alertas
                WHERE sensor_id = :sensor_id AND estacion_id = :estacion_id
                AND criterio_id = :criterio_id AND enable = 1
                ORDER BY timestamp DESC LIMIT 1
            """), {
                "sensor_id": sensor_id,
                "estacion_id": estacion_id,
                "criterio_id": criterio_id
            }).mappings().fetchone()

            if result and fecha_hora <= result["timestamp"]:
                return

            if result:
                conn.execute(text("""
                    UPDATE alertas SET 
                        contador = contador + 1,
                        valor = :valor,
                        timestamp = :timestamp,
                        timestamp_arrive = CURRENT_TIMESTAMP,
                        observacion = :observacion
                    WHERE alerta_id = :alerta_id
                """), {
                    "alerta_id": result["alerta_id"],
                    "valor": valor,
                    "timestamp": fecha_hora,
                    "observacion": observacion
                })
            else:
                conn.execute(text("""
                    INSERT INTO alertas (
                        sensor_id, estacion_id, timestamp, valor, criterio_id, contador, enable, observacion
                    ) VALUES (
                        :sensor_id, :estacion_id, :timestamp, :valor, :criterio_id, 1, 1, :observacion
                    )
                """), {
                    "sensor_id": sensor_id,
                    "estacion_id": estacion_id,
                    "timestamp": fecha_hora,
                    "valor": valor,
                    "criterio_id": criterio_id,
                    "observacion": observacion
                })
    except Exception as e:
        print(f"[ERROR] registrar_alarma_persistente: {e}")

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
        tipo_sensor = result["tipo"]

        if limite_inf is not None and limite_sup is not None:
            datos_outliers = datos[(datos["valor"] < limite_inf) | (datos["valor"] > limite_sup)]
        elif limite_inf is not None:
            datos_outliers = datos[datos["valor"] < limite_inf]
        elif limite_sup is not None:
            datos_outliers = datos[datos["valor"] > limite_sup]
        else:
            return pd.DataFrame()

        for _, fila in datos_outliers.iterrows():
            mensaje = f"Valor fuera de umbral en {nombre_estacion} ({tipo_sensor}): {fila['valor']} (umbral: {limite_inf} - {limite_sup})"
            registrar_alarma_persistente(sensor_id, estacion_id, fila['fecha_hora'], fila['valor'], criterio_id=2, observacion=mensaje)

        return datos_outliers
    except Exception as e:
        print(f"[ERROR] detectar_outliers_limites: {e}")
        return pd.DataFrame()

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
                "start_alarm": start_alarm,
                "vigency_alarm": vigency_alarm
            })
        print("[INFO] Log registrado correctamente.")
    except Exception as e:
        print(f"[ERROR] registrar_log: {e}")

# -----------------------------------------------
# BLOQUE PRINCIPAL
# -----------------------------------------------
if __name__ == "__main__":
    try:
        sensores_df = obtener_sensores_desde_umbrales()

        print("Buscando puntos críticos en últimas lecturas...")
        total_registros = 0
        total_alarmas = 0
        eventos_detectados = 0
        primer_alarma = None

        for _, sensor in sensores_df.iterrows():
            datos = obtener_ultima_lectura(sensor["sensor_id"], sensor["estacion_id"])
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

        registrar_log(
            row_vigency=len(sensores_df),
            registers=total_registros,
            event_count=eventos_detectados,
            count_alarm=total_alarmas,
            start_alarm=primer_alarma,
            vigency_alarm=datetime.now()
        )

        # Función deshabilitada temporalmente
        # if VALIDAR_GET_SENSOR_DETENIDO:
        #     print("[INFO] Validación de sensores detenidos (última lectura)...")
        #     ejecutar_get_sensor_detenido_v2(horas=4)

        print("[INFO] Proceso finalizado.")

    except Exception as e:
        print(f"[ERROR] bloque principal: {e}")