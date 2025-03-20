from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np
from pyod.models.iforest import IForest  # Modelo Isolation Forest para detección de anomalías
# Configuración de conexión a la base de datos con SQLAlchemy
DB_CONFIG = {
    "user": "root",
    "password": "1234",
    "host": "127.0.0.1",
    "port": "3306",
    "database": "gp-mlp-telemtry"  # Cambiado a la base de datos correcta
}
# Crear la cadena de conexión para SQLAlchemy
DATABASE_URL = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
engine = create_engine(DATABASE_URL)
# Función para llamar al procedimiento almacenado y obtener los datos
def obtener_datos_procedimiento(estacion_id, sensor_id, fecha):
    fecha_consulta = pd.to_datetime(fecha).strftime("%Y-%m-%d")

    with engine.connect() as conn:
        result = conn.execute(text("CALL GetSensorData(:estacion, :sensor, :periodo)"), {
            "estacion": estacion_id,
            "sensor": sensor_id,
            "periodo": 2  # Últimos 7 días
        })
        datos = result.fetchall()
        columnas = result.keys()  # Obtener los nombres de las columnas

    # Convertir a DataFrame
    df = pd.DataFrame(datos, columns=columnas)

    # Filtrar solo los datos hasta la fecha ingresada
    df = df[df["fecha_hora"] <= fecha_consulta]

    return df

# Función para detectar anomalías con PyOD
def detectar_anomalias(datos):
    if len(datos) < 10:
        print("No hay suficientes datos para detección de anomalías.")
        return []

    # Convertir datos a matriz NumPy para el modelo
    X = datos["valor"].values.reshape(-1, 1)

    # Modelo de PyOD: Isolation Forest
    modelo = IForest(contamination=0.1)  # Ajusta 'contamination' según necesidad
    modelo.fit(X)

    # Obtener predicciones
    datos["anomaly"] = modelo.predict(X)

    # Revisar los últimos 3 valores
    ultimos_3 = datos.tail(3)
    alertas = ultimos_3[ultimos_3["anomaly"] == -1]

    return alertas

# 🚀 **Código Principal**
if __name__ == "__main__":
    # Pedir datos al usuario
    fecha_revision = input("Ingrese la fecha de revisión (YYYY-MM-DD): ")
    estacion_id = int(input("Ingrese el ID de la estación: "))
    sensor_id = int(input("Ingrese el ID del sensor: "))

    # Obtener datos llamando al procedimiento almacenado
    datos = obtener_datos_procedimiento(estacion_id, sensor_id, fecha_revision)

    if datos.empty:
        print("No se encontraron datos en la base de datos.")
    else:
        # Detectar anomalías
        alertas = detectar_anomalias(datos)

        if not alertas.empty:
            print("\n🔴 ALERTA: Se detectaron anomalías en los últimos 3 registros.")
            print(alertas)
        else:
            print("\n✅ No se detectaron anomalías en los últimos 3 registros.")