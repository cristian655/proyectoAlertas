# pozos_detenidosc.py
from sqlalchemy import text
from conexion import engine
from logger import logger

def detectar_pozos_detenidos(horas=2):
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

def resolver_pozos_recuperados(horas=2):
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE `GP-MLP-Contac`.alertas a
                JOIN (
                    SELECT a.alerta_id
                    FROM `GP-MLP-Contac`.alertas a
                    JOIN `GP-MLP-Contac`.sensores s
                        ON a.sensor_id = s.sensor_id
                    JOIN `GP-MLP-Contac`.res31_mediciones d
                        ON d.tag_pi COLLATE utf8mb4_general_ci = s.tag_pi COLLATE utf8mb4_general_ci
                    WHERE a.enable = 1 AND a.criterio_id = 1
                    GROUP BY a.alerta_id
                    HAVING TIMESTAMPDIFF(
                        HOUR,
                        MAX(d.estampa_tiempo),
                        CONVERT_TZ(NOW(),'UTC','America/Santiago')
                    ) <= :horas
                ) t ON a.alerta_id = t.alerta_id
                SET a.enable = 0,
                    a.observacion = CONCAT(
                        a.observacion,
                        ' [RESUELTA ',
                        CONVERT_TZ(NOW(),'UTC','America/Santiago'),
                        ']'
                    )
            """), {"horas": horas})
            logger.info(f"[RESUELTAS] Alertas de detención resueltas correctamente en Contac (umbral={horas}h).")
    except Exception as e:
        logger.error(f"[ERROR] en resolver_pozos_recuperados en Contac: {e}")
