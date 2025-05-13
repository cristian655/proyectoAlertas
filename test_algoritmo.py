import os
import pandas as pd
from sqlalchemy import text
from datetime import datetime
from conexion import engine
from logger import logger
import importlib

# Configuración
ALGORITMO = "hotelling_test"  # nombre del archivo sin .py
FUNCION = "hotelling_T2_univariado"  # nombre de la función
DIAS_HISTORIA = 30
CARPETA_SALIDA = "resultados_pruebas"

# Crear carpeta de salida
os.makedirs(CARPETA_SALIDA, exist_ok=True)

# Importar módulo de algoritmo dinámicamente
algoritmo_modulo = importlib.import_module(ALGORITMO)
algoritmo_funcion = getattr(algoritmo_modulo, FUNCION)

def obtener_sensores_activos():
    query = text("""
        SELECT sensor_id 
        FROM ultimo_dato_sensor 
        WHERE enable = 1
    """)
    with engine.connect() as conn:
        result = conn.execute(query).fetchall()
        return [r[0] for r in result]

def obtener_datos_sensor(sensor_id, dias=DIAS_HISTORIA):
    query = text("""
        SELECT fecha_hora, valor 
        FROM Datos
        WHERE sensor_id = :sensor_id AND fecha_hora >= NOW() - INTERVAL :dias DAY
        ORDER BY fecha_hora
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {"sensor_id": sensor_id, "dias": dias})
        return pd.DataFrame(result.fetchall(), columns=["fecha_hora", "valor"])

def procesar_sensor(sensor_id):
    try:
        datos = obtener_datos_sensor(sensor_id)
        if datos.empty:
            logger.warning(f"[{sensor_id}] Sin datos disponibles.")
            return

        # Guardar datos originales
        raw_path = f"{CARPETA_SALIDA}/datos_raw_{sensor_id}.csv"
        datos.to_csv(raw_path, index=False)

        # Aplicar el algoritmo
        resultado = algoritmo_funcion(datos)

        # Guardar resultados con nombre del algoritmo
        nombre_algoritmo = ALGORITMO.replace("_test", "")
        res_path = f"{CARPETA_SALIDA}/resultados_{nombre_algoritmo}_{sensor_id}.csv"
        resultado.to_csv(res_path, index=False)

        print(f"[{sensor_id}] Proceso completo ✓")

    except Exception as e:
        logger.error(f"[{sensor_id}] Error: {e}")
        print(f"[{sensor_id}] ❌ Error al procesar: {e}")

if __name__ == "__main__":
    sensores = obtener_sensores_activos()
    print(f"Procesando {len(sensores)} sensores con el algoritmo '{ALGORITMO}'...")

    for sensor_id in sensores:
        procesar_sensor(sensor_id)

    print("✅ Proceso finalizado.")
