#!/bin/bash
echo "=== CRON EJECUTADO $(date) ===" >> /home/ec2-user/alertas/logs/cron.log
# Activar entorno virtual
source /home/ec2-user/alertas/venv/bin/activate
# Ejecutar script con Python del entorno
python /home/ec2-user/alertas/alerta.py >> /home/ec2-user/alertas/logs/cron.log 2>&1
