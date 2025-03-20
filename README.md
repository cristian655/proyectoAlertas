🔍 Detección de Anomalías en Sensores de Telemetría

📌 Descripción
Este proyecto implementa un módulo en Python para la detección de anomalías en los datos de sensores de telemetría almacenados en una base de datos MySQL. Utiliza SQLAlchemy para la conexión a la base de datos y el modelo Isolation Forest de la librería PyOD para identificar valores atípicos.

⚙️ Funcionamiento
Obtener todas las estaciones mediante el procedimiento almacenado GetEstaciones().
Obtener los sensores de cada estación usando GetSensoresByEstacion(estacion_id).
Consultar los datos de cada sensor para un periodo determinado (periodo=2) con GetSensorData(estacion_id, sensor_id, periodo).
Limpiar los datos eliminando valores nulos (NaN) en la columna valor.
Detectar anomalías en la serie de tiempo de cada sensor con Isolation Forest de PyOD.
Mostrar los puntos críticos detectados con el formato:

🛠️ Requisitos
Python 3.8+
MySQL (con acceso a la base de datos gp-mlp-telemtry)
Librerías necesarias:
  -sqlalchemy
  -pandas
  -numpy
  -pyod
  
🚀 Uso
Ejecuta el script con:
El sistema recorrerá todas las estaciones y sensores, detectando valores anómalos en cada uno y mostrando los resultados en la consola.

📂 Estructura del Código
obtener_estaciones(): Obtiene la lista de estaciones.
obtener_sensores(estacion_id): Obtiene los sensores de una estación específica.
obtener_datos(estacion_id, sensor_id): Obtiene los datos históricos del sensor.
detectar_anomalias(datos): Detecta valores atípicos en la serie temporal.

⚠️ Consideraciones
El procedimiento almacenado GetSensorData siempre devuelve datos desde la fecha actual hacia atrás.
Se recomienda ajustar el parámetro contamination en Isolation Forest según la naturaleza de los datos.
Si no hay suficientes datos (menos de 10 registros) o si los valores contienen NaN, el análisis de anomalías no se ejecutará.


