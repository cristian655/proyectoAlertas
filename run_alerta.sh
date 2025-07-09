#!/bin/bash

echo "=== CRON EJECUTADO $(date) ===" >> /home/ubuntu/proyectoAlertas/logs/cron.log

# Activar entorno virtual
source /home/ubuntu/proyectoAlertas/venv/bin/activate

# Ejecutar alerta principal
python /home/ubuntu/proyectoAlertas/alerta.py >> /home/ubuntu/proyectoAlertas/logs/cron.log 2>&1

# Ejecutar alertac (Contact)
python /home/ubuntu/proyectoAlertas/alertac.py >> /home/ubuntu/proyectoAlertas/logs/cron.log 2>&1

# Ejecutar notificación HTML con logo
python /home/ubuntu/proyectoAlertas/enviar_alertas_pendientes.py >> /home/ubuntu/proyectoAlertas/logs/cron.log 2>&1
