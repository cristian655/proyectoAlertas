# -*- coding: utf-8 -*-
import pandas as pd
from sqlalchemy import text
from conexion import engine
from logger import logger
from registro_alertas import registrar_alarma_persistente
from algoritmos_univariados import hotelling_T2_univariado, isolation_forest, rolling_zscore
from enviar_correo import notificar_alerta_modelo

def probar_anomalias_por_sensor(sensor_id, dias=30):
    query_meta = text("""
        SELECT u.estacion_id, s.tipo_raw, e.nombre AS nombre_estacion
        FROM umbrales u
        JOIN Sensores s ON u.sensor_id = s.sensor_id
        JOIN Estaciones e ON u.estacion_id = e.estacion_id
        WHERE u.sensor_id = :sensor_id
    """)
    with engine.connect() as conn:
        meta = conn.execute(query_meta, {"sensor_id": sensor_id}).mappings().fetchone()
        if not meta:
            print("[ERROR] Sensor {} no encontrado.".format(sensor_id))
            return

        estacion_id = meta["estacion_id"]
        tipo_sensor = meta["tipo_raw"]
        nombre_estacion = meta["nombre_estacion"]

        df = pd.read_sql(text("""
            SELECT fecha_hora, valor 
            FROM Datos
            WHERE sensor_id = :sensor_id AND fecha_hora >= NOW() - INTERVAL :dias DAY
            ORDER BY fecha_hora
        """), conn, params={"sensor_id": sensor_id, "dias": dias})

    if df.empty or len(df) < 24:
        print("[ADVERTENCIA] Sensor {} sin suficientes datos.".format(sensor_id))
        return

    print("\n--- Análisis para el sensor {} ({}) en estación '{}' ---".format(
        sensor_id, tipo_sensor, nombre_estacion))
    print("Último valor: {} a las {}".format(df.iloc[-1]['valor'], df.iloc[-1]['fecha_hora']))

    try:
        r1 = hotelling_T2_univariado(df)
        r2 = isolation_forest(df)
        r3 = rolling_zscore(df)

        a1 = r1["anomalía"].iloc[-1]
        a2 = r2["anomalía"].iloc[-1]
        a3 = r3["anomalía"].iloc[-1]

        print("\nResultados de los modelos:")
        print("- Hotelling T2        : {}".format("Anomalía" if a1 else "Normal"))
        print("- Isolation Forest    : {}".format("Anomalía" if a2 else "Normal"))
        print("- Rolling Z-Score     : {}".format("Anomalía" if a3 else "Normal"))

        if a1 or a2 or a3:
            fecha_alerta = df["fecha_hora"].iloc[-1]
            valor = df["valor"].iloc[-1]
            algoritmos = []
            if a1: algoritmos.append("Hotelling T²")
            if a2: algoritmos.append("Isolation Forest")
            if a3: algoritmos.append("Rolling Z-Score")

            observacion = "Anomalía detectada por: " + ", ".join(algoritmos)

            print("\n[ALERTA] Se generaría una alerta con los siguientes datos:")
            print("- Fecha/Hora: {}".format(fecha_alerta))
            print("- Valor     : {}".format(valor))
            print("- Algoritmos: {}".format(", ".join(algoritmos)))
        else:
            print("\n[OK] Ninguna anomalía detectada en el último dato.")

    except Exception as e:
        print("[ERROR] Fallo al ejecutar el análisis: {}".format(e))

# Uso
if __name__ == "__main__":
    probar_anomalias_por_sensor(sensor_id=158)
