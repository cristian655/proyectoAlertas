# -*- coding: utf-8 -*-
from sqlalchemy import create_engine, text
import pandas as pd
from datetime import datetime

from conexion import engine
from pozos_detenidos import detectar_pozos_detenidos, resolver_pozos_recuperados
from logger import logger
from enviar_correo import notificar_alerta
#from anomalias_modelos import verificar_anomalias_por_modelo
from registro_alertas import registrar_alarma_persistente
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="sklearn")



def obtener_sensores_con_umbrales():
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT 
                u.sensor_id, u.estacion_id,
                u.umbral_minimo, u.umbral_maximo,
                s.tipo, e.nombre AS nombre_estacion
            FROM umbrales u
            INNER JOIN Sensores s ON u.sensor_id = s.sensor_id
            INNER JOIN Estaciones e ON u.estacion_id = e.estacion_id
            WHERE u.tiene_umbrales = TRUE AND u.enable = 1
        """))
        return pd.DataFrame(result.fetchall(), columns=result.keys())


def obtener_ultima_lectura(sensor_id, estacion_id):
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT valor, fecha_hora
            FROM Datos
            WHERE sensor_id = :sensor_id
            ORDER BY fecha_hora DESC
            LIMIT 1
        """), {
            "sensor_id": sensor_id
        }).mappings().fetchone()

        if result:
            return result["fecha_hora"], result["valor"]
        return None, None


def verificar_alertas_activas():
    with engine.begin() as conn:
        alertas_activas = conn.execute(text("""
            SELECT 
                a.alerta_id, a.sensor_id, a.estacion_id, a.timestamp,
                u.umbral_minimo, u.umbral_maximo
            FROM Alertas a
            JOIN umbrales u 
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
                    UPDATE Alertas
                    SET enable = 0, observacion = CONCAT(observacion, ' [RESUELTA ', CURRENT_TIMESTAMP, ']')
                    WHERE alerta_id = :alerta_id
                """), {"alerta_id": alerta_id})

                logger.info("[RESUELTA] Alerta desactivada para sensor {}".format(sensor_id))
            else:
                logger.info("[VIGENTE] Alerta aun activa para sensor {}".format(sensor_id))

def desactivar_alertas_modelo():
    try:
        with engine.begin() as conn:
            conn.execute(text("CALL marcar_alertas_modelo()"))
            logger.info("[LIMPIEZA] Alertas tipo modelo (criterio_id=3) desactivadas si no se repitieron.")
    except Exception as e:
        logger.error(f"[ERROR] Fallo al desactivar alertas modelo: {e}")

#Función para invocar el procedimiento de limpieza
def limpiar_alertas_inactivas(dias: int = 15):
    try:
        with engine.begin() as conn:
            conn.execute(text("CALL limpiar_alertas_inactivas(:dias)"), {"dias": dias})
            logger.info(f"[LIMPIEZA] Alertas inactivas eliminadas (criterio_id=1, >{dias} días o todas si 0)")
    except Exception as e:
        logger.error(f"[ERROR] Fallo al ejecutar limpieza de alertas inactivas: {e}")

#Función para invocar la vista de estado de alertas
def obtener_resumen_diario():
    query = text("SELECT * FROM vista_alertas_unificada")  # Ajusta el nombre si es distinto
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    return df


# ------------------------------------
# BLOQUE PRINCIPAL
# ------------------------------------

if __name__ == "__main__":
    from anomalias_modelos import verificar_anomalias_por_modelo
    logger.info("[INICIO] Revision de sensores con umbrales...")

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
            logger.info("[INFO] Sin lectura para sensor {}".format(sensor_id))
            continue

        if (limite_inf is not None and valor < limite_inf) or (limite_sup is not None and valor > limite_sup):
            mensaje = "Valor fuera de umbral en {} ({}): {} (umbral: {} - {})".format(
                nombre_estacion, tipo, valor, limite_inf, limite_sup
            )
            registrar_alarma_persistente(sensor_id, estacion_id, fecha, valor, observacion=mensaje)
        else:
            logger.info("[OK] Sensor {} dentro de rango.".format(sensor_id))

    logger.info("[FIN] Revision de nuevos datos finalizada.\n")

    verificar_alertas_activas()
    logger.info("[FIN] Verificacion de alertas activas finalizada.")

    logger.info("[INICIO] Revision de pozos detenidos...")
    detectar_pozos_detenidos(horas=4)
    resolver_pozos_recuperados()
    logger.info("[FIN] Revision de pozos detenidos y recuperados.")
    #verificar_anomalias_por_modelo()
    desactivar_alertas_modelo()
    # 🔽 Limpieza de alertas inactivas
    limpiar_alertas_inactivas(dias=0)
