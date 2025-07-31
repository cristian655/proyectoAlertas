# alertac.py
from sqlalchemy import text
import pandas as pd
from datetime import datetime

from conexion import engine
from pozos_detenidosc import detectar_pozos_detenidos, resolver_pozos_recuperados
from logger import logger
from enviar_correo import notificar_alerta
from registro_alertasc import registrar_alarma_persistente
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="sklearn")

def obtener_sensores_con_umbrales():
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT 
                u.sensor_id, u.estacion_id,
                u.umbral_minimo, u.umbral_maximo,
                s.tipo_raw AS tipo, e.nombre AS nombre_estacion
            FROM `GP-MLP-Contac`.umbrales u
            INNER JOIN `GP-MLP-Contac`.sensores s ON u.sensor_id = s.sensor_id
            INNER JOIN `GP-MLP-Contac`.estaciones e ON u.estacion_id = e.estacion_id
            WHERE u.tiene_umbrales = TRUE AND u.enable = 1
        """))
        return pd.DataFrame(result.fetchall(), columns=result.keys())

def obtener_ultima_lectura(sensor_id, estacion_id):
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT valor_medicion, estampa_tiempo
            FROM `GP-MLP-Contac`.res31_mediciones
            WHERE tag_pi COLLATE utf8mb4_general_ci = (
                SELECT tag_pi
                FROM `GP-MLP-Contac`.sensores
                WHERE sensor_id = :sensor_id AND estacion_id = :estacion_id
            )
            ORDER BY estampa_tiempo DESC
            LIMIT 1
        """), {
            "sensor_id": sensor_id,
            "estacion_id": estacion_id
        }).mappings().fetchone()

        if result:
            return result["estampa_tiempo"], result["valor_medicion"]
        return None, None

def verificar_alertas_activas():
    with engine.begin() as conn:
        alertas_activas = conn.execute(text("""
            SELECT 
                a.alerta_id, a.sensor_id, a.estacion_id, a.timestamp,
                u.umbral_minimo, u.umbral_maximo
            FROM `GP-MLP-Contac`.alertas a
            JOIN `GP-MLP-Contac`.umbrales u 
                ON a.sensor_id = u.sensor_id AND a.estacion_id = u.estacion_id
            WHERE a.enable = 1 AND u.enable = 1 AND u.tiene_umbrales = TRUE
        """)).mappings().all()

        for alerta in alertas_activas:
            sensor_id = alerta["sensor_id"]
            estacion_id = alerta["estacion_id"]
            alerta_id = alerta["alerta_id"]
            limite_inf = alerta["umbral_minimo"]
            limite_sup = alerta["umbral_maximo"]

            fecha, valor = obtener_ultima_lectura(sensor_id, estacion_id)

            if fecha is None:
                logger.info("[INFO] Sin lectura actual para sensor {}".format(sensor_id))
                continue

            if ((limite_inf is None or valor >= limite_inf) and 
                (limite_sup is None or valor <= limite_sup)):

                conn.execute(text("""
                    UPDATE `GP-MLP-Contac`.alertas
                    SET enable = 0, observacion = CONCAT(observacion, ' [RESUELTA ', CURRENT_TIMESTAMP, ']')
                    WHERE alerta_id = :alerta_id
                """), {"alerta_id": alerta_id})

                logger.info("[RESUELTA] Alerta desactivada para sensor {}".format(sensor_id))
            else:
                logger.info("[VIGENTE] Alerta aun activa para sensor {}".format(sensor_id))

def desactivar_alertas_modelo():
    try:
        with engine.begin() as conn:
            conn.execute(text("CALL `GP-MLP-Contac`.marcar_alertas_modelo()"))
            logger.info("[LIMPIEZA] Alertas tipo modelo (criterio_id=3) desactivadas si no se repitieron.")
    except Exception as e:
        logger.error(f"[ERROR] Fallo al desactivar alertas modelo: {e}")

def limpiar_alertas_inactivas(dias: int = 15):
    try:
        with engine.begin() as conn:
            conn.execute(text("CALL `GP-MLP-Contac`.limpiar_alertas_inactivas(:dias)"), {"dias": dias})
            logger.info(f"[LIMPIEZA] Alertas inactivas eliminadas (criterio_id=1, >{dias} días o todas si 0)")
    except Exception as e:
        logger.error(f"[ERROR] Fallo al ejecutar limpieza de alertas inactivas: {e}")

def obtener_resumen_diario():
    query = text("SELECT * FROM `GP-MLP-Contac`.vista_alertas_detalle")
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    return df

if __name__ == "__main__":
    from anomalias_modelosc import verificar_anomalias_por_modelo
    from decimal import Decimal, InvalidOperation

    logger.info("[INICIO] Revisión de sensores con umbrales...")

    sensores = obtener_sensores_con_umbrales()

    for _, row in sensores.iterrows():
        sensor_id = row['sensor_id']
        estacion_id = row['estacion_id']
        nombre_estacion = row['nombre_estacion']
        tipo = row['tipo']
        limite_inf = row['umbral_minimo']
        limite_sup = row['umbral_maximo']

        fecha, valor = obtener_ultima_lectura(sensor_id, estacion_id)

        if fecha is None:
            logger.info(f"[INFO] Sin lectura para sensor {sensor_id} ({nombre_estacion})")
            continue

        try:
            valor_decimal = Decimal(valor)
            limite_inf_decimal = Decimal(limite_inf) if limite_inf is not None else None
            limite_sup_decimal = Decimal(limite_sup) if limite_sup is not None else None

            if (limite_inf_decimal is not None and valor_decimal <= limite_inf_decimal) or \
               (limite_sup_decimal is not None and valor_decimal >= limite_sup_decimal):
                mensaje = (
                    f"Valor fuera de umbral en {nombre_estacion} ({tipo}): "
                    f"{valor} (umbral: {limite_inf} - {limite_sup})"
                )
                registrar_alarma_persistente(sensor_id, estacion_id, fecha, valor, observacion=mensaje)
            else:
                logger.info(f"[OK] Sensor {sensor_id} dentro de rango.")
        except InvalidOperation as e:
            logger.error(
                f"[ERROR] Conversión fallida para sensor {sensor_id} ({nombre_estacion}) → "
                f"valor={valor}, inf={limite_inf}, sup={limite_sup} → {e}"
            )
            continue

    logger.info("[FIN] Revisión de nuevos datos finalizada.")

    # Verificación y limpieza de alertas
    verificar_alertas_activas()
    logger.info("[FIN] Verificación de alertas activas finalizada.")

    logger.info("[INICIO] Revisión de pozos detenidos...")
    detectar_pozos_detenidos(horas=4)
    resolver_pozos_recuperados()
    logger.info("[FIN] Revisión de pozos detenidos y recuperados.")

    #verificar_anomalias_por_modelo() #ESTABA LLAMANDO A LA MISMA FUNCION!!!!
    desactivar_alertas_modelo()
    limpiar_alertas_inactivas(dias=0)
