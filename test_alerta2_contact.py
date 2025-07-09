# test_alerta2_contact.py

from alertac import obtener_sensores_con_umbrales, obtener_ultima_lectura
from registro_alertasc import registrar_alarma_persistente
from logger import logger

if __name__ == "__main__":
    logger.info("[TEST] Iniciando prueba de alertas tipo 2 con archivos clonados...")

    sensores = obtener_sensores_con_umbrales()

    for _, row in sensores.iterrows():
        sensor_id = row['sensor_id']
        estacion_id = row['estacion_id']
        nombre_estacion = row['nombre_estacion']
        tipo = row['tipo']
        limite_inf = row['umbral_minimo']
        limite_sup = row['umbral_maximo']

        fecha, valor = obtener_ultima_lectura(sensor_id, estacion_id)

        if fecha is None or valor is None:
            logger.info(f"[SKIP] Sin lectura para sensor {sensor_id}")
            continue

        try:
            valor_f = float(valor)
            limite_inf_f = float(limite_inf) if limite_inf is not None else None
            limite_sup_f = float(limite_sup) if limite_sup is not None else None
        except (ValueError, TypeError) as e:
            logger.warning(f"[SKIP] Error al convertir valores numéricos para sensor {sensor_id}: {e}")
            continue

        if (limite_inf_f is not None and valor_f < limite_inf_f) or (limite_sup_f is not None and valor_f > limite_sup_f):
            mensaje = (
                f"Valor fuera de umbral en {nombre_estacion} ({tipo}): {valor_f} "
                f"(umbral: {limite_inf_f} - {limite_sup_f})"
            )
            registrar_alarma_persistente(sensor_id, estacion_id, fecha, valor_f, observacion=mensaje)
        else:
            logger.info(f"[OK] Sensor {sensor_id} dentro de rango.")

    logger.info("[TEST] Finalizada verificación de alertas tipo 2 usando módulos clonados.")
