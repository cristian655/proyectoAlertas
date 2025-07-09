#!/bin/bash
echo "=== CRON EJECUTADO $(date) ===" >> /home/ubuntu/proyectoAlertas/logs/cron.log
# Activar entorno virtual
source /home/ubuntu/proyectoAlertas/venv/bin/activate
# Ejecutar script con Python del entorno
python /home/ubuntu/proyectoAlertas/alerta.py >> /home/ubuntu/proyectoAlertas/logs/cron.log 2>&1
