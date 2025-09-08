from sqlalchemy import text
from conexion import engine
from logger import logger
from enviar_correo import notificar_alerta

def registrar_alarma_persistente(sensor_id, estacion_id, fecha_hora, valor, criterio_id=2, observacion=None, algoritmos_detectores=None):

    with engine.begin() as conn:
        nombre_estacion = conn.execute(text("""
            SELECT nombre FROM `GP-MLP-Contac`.estaciones WHERE estacion_id = :id
        """), {"id": estacion_id}).scalar()

        tipo_sensor = conn.execute(text("""
            SELECT tipo_raw FROM `GP-MLP-Contac`.sensores WHERE sensor_id = :id
        """), {"id": sensor_id}).scalar()

        ultima = conn.execute(text("""
            SELECT alerta_id, timestamp, contador
            FROM `GP-MLP-Contac`.alertas
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
            logger.info(f"[OMITIDA] sensor {sensor_id} → nueva={fecha_hora}, ultima={ultima['timestamp']}")
            return

        if ultima:
            conn.execute(text("""
                UPDATE `GP-MLP-Contac`.alertas SET 
                    contador = contador + 1,
                    valor = :valor,
                    timestamp = :fecha_hora,
                    observacion = :observacion
                WHERE alerta_id = :alerta_id
            """), {
                "alerta_id": ultima["alerta_id"],
                "valor": valor,
                "fecha_hora": fecha_hora,
                "observacion": observacion
            })
            logger.warning(f"[ALERTA] Alarma actualizada para sensor {sensor_id}")
            notificar_alerta(tipo_sensor, nombre_estacion, valor, ultima["contador"] + 1, fecha_hora)

        else:
            fecha_llegada = fecha_hora

            conn.execute(text("""
                INSERT INTO `GP-MLP-Contac`.alertas (
                    sensor_id, estacion_id, timestamp, valor, criterio_id, contador, enable, observacion, timestamp_arrive
                ) VALUES (
                    :sensor_id, :estacion_id, :fecha_hora, :valor, :criterio_id, 1, 1, :observacion, :fecha_llegada
                )
            """), {
                "sensor_id": sensor_id,
                "estacion_id": estacion_id,
                "fecha_hora": fecha_hora,
                "valor": valor,
                "criterio_id": criterio_id,
                "observacion": observacion,
                "fecha_llegada": fecha_llegada
            })
            logger.warning(f"[ALERTA] Nueva alarma registrada para sensor {sensor_id}")
            notificar_alerta(tipo_sensor, nombre_estacion, valor, 1, fecha_hora)
