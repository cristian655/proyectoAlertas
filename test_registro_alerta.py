# test_registro_alerta.py
from datetime import datetime, timedelta
from registro_alertas import registrar_alarma_persistente

sensor_id = 157
estacion_id = 999  # ⚠️ reemplaza por el `estacion_id` real de este sensor en tu BD
fecha = datetime.now() + timedelta(seconds=1)  # fuerza un timestamp nuevo
valor = 999
criterio = 3
observacion = "⚠️ Alerta de prueba manual por modelo (sensor 157)"

registrar_alarma_persistente(
    sensor_id=sensor_id,
    estacion_id=estacion_id,
    fecha_hora=fecha,
    valor=valor,
    criterio_id=criterio,
    observacion=observacion
)
