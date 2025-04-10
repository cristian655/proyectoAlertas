from sqlalchemy import create_engine, text
import pandas as pd
from datetime import datetime

# Conexión a la base de datos
DB_CONFIG = {
    "user": "master_admin",
    "password": "LtH40j907uN0hwPm9Xoo",
    "host": "db-gp-mlp-dev-01.cn24ekkaknsg.us-east-1.rds.amazonaws.com",
    "port": "3306",
    "database": "GP-MLP-Telemtry"
}
DATABASE_URL = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
engine = create_engine(DATABASE_URL)

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

def registrar_alarma_persistente(sensor_id, estacion_id, fecha_hora, valor, criterio_id=2, observacion=None):
    with engine.begin() as conn:
        ultima = conn.execute(text("""
            SELECT alerta_id, timestamp
            FROM Alertas
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
            print(f"[INFO] Alarma ya registrada para sensor {sensor_id}, se omite.")
            return

        if ultima:
            conn.execute(text("""
                UPDATE Alertas SET 
                    contador = contador + 1,
                    valor = :valor,
                    timestamp = :fecha_hora,
                    timestamp_arrive = CURRENT_TIMESTAMP,
                    observacion = :observacion
                WHERE alerta_id = :alerta_id
            """), {
                "alerta_id": ultima["alerta_id"],
                "valor": valor,
                "fecha_hora": fecha_hora,
                "observacion": observacion
            })
            print(f"[ALERTA] Alarma actualizada para sensor {sensor_id}")
        else:
            conn.execute(text("""
                INSERT INTO Alertas (
                    sensor_id, estacion_id, timestamp, valor, criterio_id, contador, enable, observacion
                ) VALUES (
                    :sensor_id, :estacion_id, :fecha_hora, :valor, :criterio_id, 1, 1, :observacion
                )
            """), {
                "sensor_id": sensor_id,
                "estacion_id": estacion_id,
                "fecha_hora": fecha_hora,
                "valor": valor,
                "criterio_id": criterio_id,
                "observacion": observacion
            })
            print(f"[ALERTA] Nueva alarma registrada para sensor {sensor_id}")

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
                print(f"[INFO] Sin lectura actual para sensor {sensor_id}")
                continue

            if ((limite_inf is None or valor >= limite_inf) and 
                (limite_sup is None or valor <= limite_sup)):
                
                conn.execute(text("""
                    UPDATE Alertas
                    SET enable = 0, observacion = CONCAT(observacion, ' [RESUELTA ', CURRENT_TIMESTAMP, ']')
                    WHERE alerta_id = :alerta_id
                """), {"alerta_id": alerta_id})

                print(f"[RESUELTA] Alerta desactivada para sensor {sensor_id}")
            else:
                print(f"[VIGENTE] Alerta aún activa para sensor {sensor_id}")

# ------------------------------------
# BLOQUE PRINCIPAL
# ------------------------------------
if __name__ == "__main__":
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
            print(f"[INFO] Sin lectura para sensor {sensor_id}")
            continue

        if (limite_inf is not None and valor < limite_inf) or (limite_sup is not None and valor > limite_sup):
            mensaje = f"Valor fuera de umbral en {nombre_estacion} ({tipo}): {valor} (umbral: {limite_inf} - {limite_sup})"
            registrar_alarma_persistente(sensor_id, estacion_id, fecha, valor, observacion=mensaje)
        else:
            print(f"[OK] Sensor {sensor_id} dentro de rango.")

    print("[FIN] Revisión de nuevos datos finalizada.\n")

    verificar_alertas_activas()

    print("[FIN] Verificación de alertas activas finalizada.")
