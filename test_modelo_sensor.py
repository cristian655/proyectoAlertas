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
            print(f"❌ Sensor {sensor_id} no encontrado.")
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
        print(f"⚠️ Sensor {sensor_id} sin suficientes datos.")
        return

    print(f"\n📊 Analizando sensor {sensor_id} ({tipo_sensor}) de estación '{nombre_estacion}'")
    print(f"Último valor registrado: {df.iloc[-1]['valor']} a las {df.iloc[-1]['fecha_hora']}")

    try:
        r1 = hotelling_T2_univariado(df)
        r2 = isolation_forest(df)
        r3 = rolling_zscore(df)

        a1 = r1["anomalía"].iloc[-1]
        a2 = r2["anomalía"].iloc[-1]
        a3 = r3["anomalía"].iloc[-1]

        print(f"\n🔍 Resultados:")
        print(f"- Hotelling T²         → {'✅ Anomalía' if a1 else '✔️ Normal'}")
        print(f"- Isolation Forest     → {'✅ Anomalía' if a2 else '✔️ Normal'}")
        print(f"- Rolling Z-Score      → {'✅ Anomalía' if a3 else '✔️ Normal'}")

        if any([a1, a2, a3]):
            fecha_alerta = df["fecha_hora"].iloc[-1]
            valor = df["valor"].iloc[-1]
            algoritmos = []
            if a1: algoritmos.append("Hotelling T²")
            if a2: algoritmos.append("Isolation Forest")
            if a3: algoritmos.append("Rolling Z-Score")

            observacion = "Anomalía detectada por: " + ", ".join(algoritmos)

            print(f"\n🚨 Se generaría una alerta con: {observacion}")
            print(f"Fecha/hora: {fecha_alerta}, valor: {valor}")
        else:
            print("\n✅ Ninguna anomalía detectada por los modelos.")

    except Exception as e:
        print(f"❌ Error al ejecutar análisis: {e}")

# Ejemplo de uso interactivo:
if __name__ == "__main__":
    probar_anomalias_por_sensor(sensor_id=158)
