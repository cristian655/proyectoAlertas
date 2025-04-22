# hotelling_test.py
import pandas as pd
import numpy as np
from sqlalchemy import text
from conexion import engine
from logger import logger


def obtener_datos_historicos(sensor_id, dias=30):
    query = text("""
        SELECT fecha_hora, valor 
        FROM Datos
        WHERE sensor_id = :sensor_id AND fecha_hora >= NOW() - INTERVAL :dias DAY
        ORDER BY fecha_hora
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {"sensor_id": sensor_id, "dias": dias})
        return pd.DataFrame(result.fetchall(), columns=result.keys())


def hotelling_T2_univariado(df, alpha=0.01):
    x = df['valor']
    mean = x.mean()
    std = x.std()
    T2 = ((x - mean) / std) ** 2
    umbral = np.percentile(T2, 100 * (1 - alpha))
    df["T2"] = T2
    df["anomalía"] = df["T2"] > umbral
    return df


if __name__ == "__main__":
    sensor_id = 123  # ← reemplaza este ID
    dias_historial = 30

    try:
        datos = obtener_datos_historicos(sensor_id, dias=dias_historial)
        if datos.empty:
            logger.warning("No se encontraron datos para el sensor.")
            print("⚠️ No se encontraron datos.")
        else:
            resultado = hotelling_T2_univariado(datos)
            print(resultado[resultado["anomalía"]])
    except Exception as e:
        logger.error(f"Error en prueba Hotelling: {e}")
        print(f"❌ Error al ejecutar: {e}")
