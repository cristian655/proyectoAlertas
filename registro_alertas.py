from sqlalchemy import text
from conexion import engine
from logger import logger
from enviar_correo import notificar_alerta  # notificar_alerta_modelo removido

def registrar_alarma_persistente(sensor_id, estacion_id, fecha_hora, valor, criterio_id=2, observacion=None, algoritmos_detectores=None):
    with engine.begin() as conn:
        nombre_estacion = conn.execute(text("""
            SELECT nombre FROM Estaciones WHERE estacion_id = :id
        """), {"id": estacion_id}).scalar()

        tipo_sensor = conn.execute(text("""
            SELECT tipo_raw FROM Sensores WHERE sensor_id = :id
        """), {"id": sensor_id}).scalar()

        ultima = conn.execute(text("""
            SELECT alerta_id, timestamp, contador
            FROM Alertas
            WHERE sensor_id = :sensor_id AND estacion_id = :estacion_id
              AND criterio_id = :criterio_id AND enable = 1
            ORDER BY timestamp DESC
            LIMIT 1
        """), {
            "sensor_id": sensor_id,
            "estacion_id": estacion_id,
            "criterio_id": criterio_id
        }).mappings().fetchone()

        if ultima and fecha_hora <= ultima["timestamp"]:
            logger.info("[OMITIDA] Alarma ya registrada para sensor {}, se omite.".format(sensor_id))
            return

        if ultima:
            conn.execute(text("""
                UPDATE Alertas SET 
                    contador = contador + 1,
                    valor = :valor,
                    timestamp = :fecha_hora,
                    timestamp_arrive = CONVERT_TZ(NOW(), 'UTC', 'America/Santiago'),
                    observacion = :observacion
                WHERE alerta_id = :alerta_id
            """), {
                "alerta_id": ultima["alerta_id"],
                "valor": valor,
                "fecha_hora": fecha_hora,
                "observacion": observacion
            })
            logger.warning("[ALERTA] Alarma actualizada para sensor {}".format(sensor_id))

            if criterio_id == 3:
                logger.info(f"[MODELO] Alarma actualizada (sin correo) para {nombre_estacion} - {tipo_sensor} con valor {valor}")
            else:
                notificar_alerta(tipo_sensor, nombre_estacion, valor, ultima["contador"] + 1, fecha_hora)

        else:
            conn.execute(text("""
                INSERT INTO Alertas (
                    sensor_id, estacion_id, timestamp, valor, criterio_id, contador, enable, observacion
                ) VALUES (
                    :sensor_id, :estacion_id, :fecha_hora, :valor, :criterio_id, 1, 1, :observacion
                )
            """), {
                "sensor_id": sensor_id,
                "estacion_id": estacion_id,
                "fecha_hora": fecha_hora,
                "valor": valor,
                "criterio_id": criterio_id,
                "observacion": observacion
            })
            logger.warning("[ALERTA] Nueva alarma registrada para sensor {}".format(sensor_id))

            if criterio_id == 3:
                logger.info(f"[MODELO] Nueva alarma registrada (sin correo) para {nombre_estacion} - {tipo_sensor} con valor {valor}")
            else:
                notificar_alerta(tipo_sensor, nombre_estacion, valor, 1, fecha_hora)
