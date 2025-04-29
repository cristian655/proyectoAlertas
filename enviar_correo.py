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

DESTINATARIOS_POR_DEFECTO = ["erivas@gpconsultores.cl", "cgonzalez@gpconsultores.cl","crgonzalezh@gmail.com"]
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
