from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np
from pyod.models.iforest import IForest  # Modelo Isolation Forest para detección de anomalías

# Configuración de conexión a la base de datos
DB_CONFIG = {
    "user": "root",
    "password": "1234",
    "host": "127.0.0.1",
    "port": "3306",
    "database": "gp-mlp-telemtry"
}

# Crear la cadena de conexión
DATABASE_URL = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
engine = create_engine(DATABASE_URL)

# Obtener todas las estaciones
def obtener_estaciones():
    with engine.connect() as conn:
        result = conn.execute(text("CALL `gp-mlp-telemtry`.GetEstaciones()"))
        datos = result.fetchall()
        columnas = result.keys()
    return pd.DataFrame(datos, columns=columnas)[["estacion_id", "nombre"]]

# Obtener todos los sensores de una estación
def obtener_sensores(estacion_id):
    with engine.connect() as conn:
        result = conn.execute(text("CALL `gp-mlp-telemtry`.GetSensoresByEstacion(:estacion)"), {"estacion": estacion_id})
        datos = result.fetchall()
        columnas = result.keys()
    return pd.DataFrame(datos, columns=columnas)[["sensor_id", "estacion_id", "tipo"]]

# Obtener datos de un sensor específico
def obtener_datos(estacion_id, sensor_id):
    with engine.connect() as conn:
        result = conn.execute(text("CALL `gp-mlp-telemtry`.GetSensorData(:estacion, :sensor, :periodo)"), {
            "estacion": estacion_id,
            "sensor": sensor_id,
            "periodo": 3  # Últimos datos disponibles
        })
        datos = result.fetchall()
        columnas = result.keys()
    df = pd.DataFrame(datos, columns=columnas)[["fecha_hora", "valor"]]
    
    # Manejo de valores NaN en la columna 'valor'
    df.dropna(subset=["valor"], inplace=True)
    
    return df

# Función para detectar outliers según límites de la base de datos
def detectar_outliers_limites(estacion_id, sensor_id, datos):
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT limite_inferior, limite_superior, tipo
            FROM `gp-mlp-telemtry`.Sensores 
            WHERE sensor_id = :sensor_id AND estacion_id = :estacion_id
        """), {"sensor_id": sensor_id, "estacion_id": estacion_id}).mappings().fetchone()
    
    if not result:
        print(f"[Advertencia] Sensor {sensor_id} no tiene límites definidos.")
        return pd.DataFrame()

    limite_inf = result["limite_inferior"]
    limite_sup = result["limite_superior"]
    tipo = result["tipo"]

    # Filtrar outliers según los límites
    if limite_inf is not None and limite_sup is not None:
        datos_outliers = datos[(datos["valor"] < limite_inf) | (datos["valor"] > limite_sup)]
    elif limite_inf is not None:
        datos_outliers = datos[datos["valor"] < limite_inf]
    elif limite_sup is not None:
        datos_outliers = datos[datos["valor"] > limite_sup]
    else:
        print(f"[Advertencia] Sensor {sensor_id} no tiene ningún límite definido.")
        return pd.DataFrame()

    for _, fila in datos_outliers.iterrows():
        print(f"[Outlier-Límites] Sensor: {sensor_id}, Estación: {estacion_id}, Tipo: {tipo}, Fecha: {fila['fecha_hora']}, Valor: {fila['valor']}, Límites: [{limite_inf}, {limite_sup}]")
    
    return datos_outliers
# Función para detectar anomalías con PyOD
def detectar_anomalias(datos):
    if len(datos) < 10:
        return pd.DataFrame()
    
    X = datos["valor"].values.reshape(-1, 1)
    
    if np.isnan(X).any():
        return pd.DataFrame()
    
    modelo = IForest(contamination=0.01)
    modelo.fit(X)
    datos["anomaly"] = modelo.predict(X)
    return datos[datos["anomaly"] == 1]  # 1 indica anomalía en PyOD

# Programa principal
if __name__ == "__main__":
    estaciones = obtener_estaciones()
    sensores_totales = []
    
    for _, estacion in estaciones.iterrows():
        sensores = obtener_sensores(estacion["estacion_id"])
        sensores_totales.append(sensores)
    
    sensores_df = pd.concat(sensores_totales, ignore_index=True)
    
    print("Buscando puntos críticos...")
    for _, sensor in sensores_df.iterrows():
        datos = obtener_datos(sensor["estacion_id"], sensor["sensor_id"])
        if not datos.empty:
            # Detectar outliers según límites
            outliers_limites = detectar_outliers_limites(sensor["estacion_id"], sensor["sensor_id"], datos)

            # Detectar anomalías con Isolation Forest
            #anomalías = detectar_anomalias(datos)
            
           # if not anomalías.empty:
            #    for _, fila in anomalías.iterrows():
             #       print(f"[Anomalía-Modelo] Sensor: {sensor['sensor_id']}, Estación: {sensor['estacion_id']}, Tipo: {sensor['tipo']}, Fecha: {fila['fecha_hora']}, Valor: {fila['valor']}")
