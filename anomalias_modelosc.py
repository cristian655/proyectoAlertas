# anomalias_modelosc.py

import pandas as pd
from sqlalchemy import text
from conexion import engine
from logger import logger
from registro_alertasc import registrar_alarma_persistente
from algoritmos_univariados import hotelling_T2_univariado, isolation_forest, rolling_zscore
from enviar_correo import notificar_alerta_modelo

def verificar_anomalias_por_modelo():
    logger.info("[INICIO] Verificación por modelos de anomalía (Contac)...")

    with engine.connect() as conn:
        sensores = conn.execute(text("""
            SELECT u.sensor_id, u.estacion_id, s.tipo_raw, e.nombre AS nombre_estacion
            FROM `GP-MLP-Contac`.umbrales u
            JOIN `GP-MLP-Contac`.ultimo_dato_sensor uds ON u.sensor_id = uds.sensor_id
            JOIN `GP-MLP-Contac`.sensores s ON u.sensor_id = s.sensor_id
            JOIN `GP-MLP-Contac`.estaciones e ON u.estacion_id = e.estacion_id
            WHERE u.usar_modelos = 1 AND u.enable = 1 AND uds.enable = 1
        """)).mappings().all()

    for sensor in sensores:
        sensor_id = sensor["sensor_id"]
        estacion_id = sensor["estacion_id"]
        tipo_sensor = sensor["tipo_raw"]
        nombre_estacion = sensor["nombre_estacion"]

        query = text("""
            SELECT estampa_tiempo AS fecha_hora, valor_medicion AS valor
            FROM `GP-MLP-Contac`.res31_mediciones
            WHERE tag_pi = (
                SELECT tag_pi
                FROM `GP-MLP-Contac`.sensores
                WHERE sensor_id = :sensor_id
                LIMIT 1
            )
            AND estampa_tiempo >= NOW() - INTERVAL 30 DAY
            ORDER BY estampa_tiempo
        """)

        with engine.connect() as conn:
            df = pd.DataFrame(conn.execute(query, {"sensor_id": sensor_id}).fetchall(), columns=["fecha_hora", "valor"])

        if df.empty or len(df) < 24:
            logger.info(f"[SKIP] Sensor {sensor_id} sin suficientes datos.")
            continue

        try:
            # DEBUG opcional para verificar los últimos datos
            logger.debug(f"[DATA] Últimos registros para sensor {sensor_id}:\n{df.tail()}")

            algoritmos_detectores = []

            resultado_hotelling = hotelling_T2_univariado(df)["anomalía"].iloc[-1]
            resultado_iso = isolation_forest(df)["anomalía"].iloc[-1]
            resultado_z = rolling_zscore(df)["anomalía"].iloc[-1]

            if resultado_hotelling:
                algoritmos_detectores.append("Hotelling T²")
            if resultado_iso:
                algoritmos_detectores.append("Isolation Forest")
            if resultado_z:
                algoritmos_detectores.append("Rolling Z-Score")

            if algoritmos_detectores:
                fecha_hora = df["fecha_hora"].iloc[-1]
                valor = df["valor"].iloc[-1]
                observacion = "Anomalía detectada por: " + ", ".join(algoritmos_detectores)

                registrar_alarma_persistente(
                    sensor_id=sensor_id,
                    estacion_id=estacion_id,
                    fecha_hora=fecha_hora,
                    valor=valor,
                    criterio_id=3,
                    observacion=observacion
                )

                # Descomenta si ya quieres enviar correo
                # notificar_alerta_modelo(nombre_estacion, tipo_sensor, valor, fecha_hora, algoritmos_detectores)
            else:
                logger.info(f"[OK] Sensor {sensor_id} sin anomalía según modelos.")

        except Exception as e:
            logger.error(f"[ERROR] Fallo en análisis IA para sensor {sensor_id}: {e}")

    logger.info("[FIN] Verificación por modelos de anomalía (Contac).")
