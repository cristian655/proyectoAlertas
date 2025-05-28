# enviar_correo.py
import os
from dotenv import load_dotenv
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from logger import logger

load_dotenv()

ENVIAR_CORREO = True

REMITENTE = "erivas@gpconsultores.cl"
SMTP_SERVER = "email-smtp.us-east-2.amazonaws.com"
SMTP_PORT = 587
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")

DESTINATARIOS_POR_DEFECTO = [ "cgonzalez@gpconsultores.cl","erivas@gpconsultores.cl"]
UMBRAL_ENVIO_REPETICION = 3

def enviar_correo(destinatarios, asunto, cuerpo):
    if not ENVIAR_CORREO:
        logger.info("[EMAIL] Envío de correos deshabilitado por configuración.")
        return

    msg = MIMEMultipart()
    msg["From"] = REMITENTE
    msg["To"] = ", ".join(destinatarios)
    msg["Subject"] = asunto
    msg.attach(MIMEText(cuerpo, "plain"))

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as servidor:
            servidor.starttls()
            servidor.login(SMTP_USER, SMTP_PASS)
            servidor.sendmail(REMITENTE, destinatarios, msg.as_string())
            logger.info(f"[EMAIL] Correo enviado a {destinatarios}.")
    except Exception as e:
        logger.error(f"[EMAIL] Error al enviar correo: {e}")

def enviar_correo_html(destinatarios, asunto, cuerpo_html):
    if not ENVIAR_CORREO:
        logger.info("[EMAIL] Envío de correos deshabilitado por configuración.")
        return

    msg = MIMEMultipart("alternative")
    msg["From"] = REMITENTE
    msg["To"] = ", ".join(destinatarios)
    msg["Subject"] = asunto
    msg.attach(MIMEText(cuerpo_html, "html"))

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as servidor:
            servidor.starttls()
            servidor.login(SMTP_USER, SMTP_PASS)
            servidor.sendmail(REMITENTE, destinatarios, msg.as_string())
            logger.info(f"[EMAIL] Correo HTML enviado a {destinatarios}.")
    except Exception as e:
        logger.error(f"[EMAIL] Error al enviar correo HTML: {e}")

def notificar_alerta(tipo_sensor, nombre_estacion, valor, contador, fecha_hora):
    asunto = "⚠️ Alerta de Umbral Superado"
    cuerpo = (
        f"Se ha detectado un valor fuera de rango en el sensor {tipo_sensor} "
        f"de la estación {nombre_estacion}. Valor: {valor}.\n\n"
    )

    if contador == 1:
        cuerpo += f"⚠️ Esta es la primera vez que se detecta esta alerta. 🕒 ({fecha_hora})"
        enviar_correo(DESTINATARIOS_POR_DEFECTO, asunto, cuerpo)

    elif contador == UMBRAL_ENVIO_REPETICION:
        cuerpo += f"⏳ La alerta ha persistido durante {UMBRAL_ENVIO_REPETICION} revisiones consecutivas hasta a las {fecha_hora}"
        enviar_correo(DESTINATARIOS_POR_DEFECTO, asunto, cuerpo)

def notificar_alerta_modelo(tipo_sensor, nombre_estacion, valor, fecha_hora):
    asunto = "📡 Anomalía detectada por Modelo IA"

    cuerpo = (
        f"📡 Se ha detectado una anomalía en el sensor {tipo_sensor} "
        f"de la estación {nombre_estacion} mediante algoritmos estadísticos.\n\n"
        f"Valor detectado: {valor}\n"
        f"Fecha y hora de la medición: {fecha_hora}\n\n"
        f"⚠️ Esta alerta fue generada por análisis de modelos IA, no por umbrales definidos."
    )

    enviar_correo(DESTINATARIOS_POR_DEFECTO, asunto, cuerpo)


def probar_envio_correo():
    asunto = "📧 Prueba de Envío desde Amazon SES"
    cuerpo = (
        "Hola,\n\n"
        "Este es un correo de prueba enviado desde el sistema de alertas usando Amazon SES via SMTP.\n\n"
        "Saludos,\nSistema de Monitoreo"
    )
    enviar_correo(DESTINATARIOS_POR_DEFECTO, asunto, cuerpo)


# Ejecutar prueba directa si se corre este script
if __name__ == "__main__":
    probar_envio_correo()
