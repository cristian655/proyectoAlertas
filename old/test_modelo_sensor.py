# -*- coding: utf-8 -*-
import pandas as pd
from sqlalchemy import text
from conexion import engine
from algoritmos_univariados import hotelling_T2_univariado, isolation_forest, rolling_zscore

def revisar_anomalias_en_lote(sensor_ids, dias=30):
    resultados = []

    for sensor_id in sensor_ids:
        query_meta = text("""
            SELECT u.estacion_id, s.tipo_raw, e.nombre AS nombre_estacion
            FROM umbrales u
            JOIN Sensores s ON u.sensor_id = s.sensor_id
            JOIN Estaciones e ON u.estacion_id = e.estacion_id
            WHERE u.sensor_id = :sensor_id
        """)
        with engine.connect() as conn:
            meta = conn.execute(query_meta, {"sensor_id": int(sensor_id)}).mappings().fetchone()
            if not meta:
                print("[ERROR] Sensor {} no encontrado.".format(sensor_id))
                continue

            estacion_id = meta["estacion_id"]
            tipo_sensor = meta["tipo_raw"]
            nombre_estacion = meta["nombre_estacion"]

            df = pd.read_sql(text("""
                SELECT fecha_hora, valor 
                FROM Datos
                WHERE sensor_id = :sensor_id AND fecha_hora >= NOW() - INTERVAL :dias DAY
                ORDER BY fecha_hora
            """), conn, params={"sensor_id": int(sensor_id), "dias": dias})

        if df.empty or len(df) < 24:
            print("[SKIP] Sensor {} sin suficientes datos.".format(sensor_id))
            continue

        try:
            r1 = hotelling_T2_univariado(df)
            r2 = isolation_forest(df)
            r3 = rolling_zscore(df)

            a1 = r1["anomalía"].iloc[-1]
            a2 = r2["anomalía"].iloc[-1]
            a3 = r3["anomalía"].iloc[-1]

            resultado = {
                "sensor_id": sensor_id,
                "estacion": nombre_estacion,
                "tipo": tipo_sensor,
                "Hotelling_T2": a1,
                "Isolation_Forest": a2,
                "Rolling_ZScore": a3
            }
            resultados.append(resultado)

        except Exception as e:
            print("[ERROR] Fallo en sensor {}: {}".format(sensor_id, e))

    return pd.DataFrame(resultados)

if __name__ == "__main__":
    # Lista fija de sensores
    sensor_ids = [
        109, 110, 114, 116, 119, 120, 122, 124, 125, 127,
        128, 129, 131, 132, 133, 135, 136, 137, 139, 140,
        141, 143, 145, 146, 148, 150, 152, 153, 155, 157,
        158, 160, 162, 163, 165, 167, 168, 170, 171, 172,
        174, 175, 176, 178, 179, 180, 181, 182, 184, 185,
        186, 188, 189, 190, 192, 193, 194, 196, 197, 198,
        200, 201, 202, 204, 205, 206, 208, 209, 210, 212,
        214, 215, 217, 218, 219, 221, 281, 282, 284
    ]

    resultados = revisar_anomalias_en_lote(sensor_ids)

    print("\n--- Resultados de sensores con anomalías ---")
    print(resultados[resultados[["Hotelling_T2", "Isolation_Forest", "Rolling_ZScore"]].any(axis=1)])

