# anomalias_modelos.py

import pandas as pd
from sqlalchemy import text
from conexion import engine
from logger import logger
#from alerta import registrar_alarma_persistente
from registro_alertas import registrar_alarma_persistente
from algoritmos_univariados import hotelling_T2_univariado, isolation_forest, rolling_zscore

def verificar_anomalias_por_modelo():
    logger.info("[INICIO] Verificación por modelos de anomalía...")

    with engine.connect() as conn:
        sensores = conn.execute(text("""
            SELECT s.sensor_id, s.tipo_raw, e.estacion_id
            FROM Sensores s
            JOIN Estaciones e ON s.estacion_id = e.estacion_id
            JOIN ultimo_dato_sensor uds ON s.sensor_id = uds.sensor_id
            WHERE uds.enable = 1
        """)).mappings().all()

    for sensor in sensores:
        sensor_id = sensor["sensor_id"]
        estacion_id = sensor["estacion_id"]

        # Obtener últimos 30 días
        query = text("""
            SELECT fecha_hora, valor 
            FROM Datos
            WHERE sensor_id = :sensor_id AND fecha_hora >= NOW() - INTERVAL 30 DAY
            ORDER BY fecha_hora
        """)
        with engine.connect() as conn:
            df = pd.DataFrame(conn.execute(query, {"sensor_id": sensor_id}).fetchall(), columns=["fecha_hora", "valor"])

        if df.empty or len(df) < 24:
            logger.info(f"[SKIP] Sensor {sensor_id} sin suficientes datos.")
            continue

        try:
            # Aplicar los tres algoritmos
            resultados = {
                "hotelling": bool(hotelling_T2_univariado(df)["anomalía"].iloc[-1]),
                "iso": bool(isolation_forest(df)["anomalía"].iloc[-1]),
                "z": bool(rolling_zscore(df)["anomalía"].iloc[-1]),
            }

            if any(resultados.values()):
                fecha_hora = df["fecha_hora"].iloc[-1]
                valor = df["valor"].iloc[-1]
                registrar_alarma_persistente(
                    sensor_id=sensor_id,
                    estacion_id=estacion_id,
                    fecha_hora=fecha_hora,
                    valor=valor,
                    criterio_id=3,
                    observacion="Anomalía detectada por al menos un modelo IA"
                )
            else:
                logger.info(f"[OK] Sensor {sensor_id} sin anomalía según modelos.")
        except Exception as e:
            logger.error(f"[ERROR] Fallo en análisis IA para sensor {sensor_id}: {e}")

    logger.info("[FIN] Verificación por modelos de anomalía.")
