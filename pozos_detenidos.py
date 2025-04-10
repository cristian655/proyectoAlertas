from sqlalchemy import text
from conexion import engine
from logger import logger

def detectar_pozos_detenidos(horas=4):
    try:
        with engine.begin() as conn:
            conn.execute(text("CALL marcar_pozos_detenidos(:horas)"), {"horas": horas})
            msg = f"Procedimiento 'marcar_pozos_detenidos' ejecutado con parámetro horas={horas}"
            print(f"[INFO] {msg}")
            logger.info(msg)
    except Exception as e:
        msg = f"Error al ejecutar procedimiento detectar_pozos_detenidos: {e}"
        print(f"[ERROR] {msg}")
        logger.error(msg)

def resolver_pozos_recuperados():
    try:
        with engine.begin() as conn:
            alertas = conn.execute(text("""
                SELECT a.alerta_id, a.sensor_id, MAX(d.fecha_hora) AS ultima
                FROM Alertas a
                JOIN Datos d ON a.sensor_id = d.sensor_id
                WHERE a.criterio_id = 1 AND a.enable = 1
                GROUP BY a.alerta_id, a.sensor_id
                HAVING TIMESTAMPDIFF(HOUR, ultima, NOW()) <= 4
            """)).mappings().all()

            for alerta in alertas:
                conn.execute(text("""
                    UPDATE Alertas
                    SET enable = 0, observacion = CONCAT(observacion, ' [RESUELTA ', CURRENT_TIMESTAMP, ']')
                    WHERE alerta_id = :id
                """), {"id": alerta["alerta_id"]})
                msg = f"Sensor {alerta['sensor_id']} volvió a emitir datos (alerta resuelta)."
                print(f"[RESUELTA] {msg}")
                logger.info(msg)
    except Exception as e:
        msg = f"Error en resolver_pozos_recuperados: {e}"
        print(f"[ERROR] {msg}")
        logger.error(msg)
