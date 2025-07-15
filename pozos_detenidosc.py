# pozos_detenidosc.py
from sqlalchemy import text
from conexion import engine
from logger import logger

def detectar_pozos_detenidos(horas=3):
    try:
        with engine.begin() as conn:
            conn.execute(text("CALL `GP-MLP-Contac`.marcar_pozos_detenidos(:horas)"), {"horas": horas})
            msg = f"Procedimiento 'marcar_pozos_detenidos' ejecutado en esquema Contac con horas={horas}"
            print(f"[INFO] {msg}")
            logger.info(msg)
    except Exception as e:
        msg = f"Error al ejecutar procedimiento detectar_pozos_detenidos en Contac: {e}"
        print(f"[ERROR] {msg}")
        logger.error(msg)

def resolver_pozos_recuperados():
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE `GP-MLP-Contac`.alertas a
                JOIN (
                    SELECT a.alerta_id
                    FROM `GP-MLP-Contac`.alertas a
                    JOIN `GP-MLP-Contac`.res31_mediciones d ON a.sensor_id = d.sensor_id
                    WHERE a.enable = 1 AND a.criterio_id = 1
                    GROUP BY a.alerta_id
                    HAVING TIMESTAMPDIFF(HOUR, MAX(d.fecha_hora), CONVERT_TZ(NOW(), 'UTC', 'America/Santiago')) <= 4
                ) t ON a.alerta_id = t.alerta_id
                SET a.enable = 0,
                    a.observacion = CONCAT(a.observacion, ' [RESUELTA ', NOW(), ']')
            """))
            logger.info("[RESUELTAS] Alertas de detención resueltas correctamente en Contac.")
    except Exception as e:
        logger.error(f"[ERROR] en resolver_pozos_recuperados en Contac: {e}")
