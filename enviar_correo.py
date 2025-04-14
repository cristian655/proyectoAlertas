# enviar_correo.py
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from logger import logger

ENVIAR_CORREO = True  # True para habilitar

REMITENTE = "crgonzalezh@gmail.com"
PASSWORD = "cids nbjo nclr vfnt"  # mover a una variable de entorno
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587

DESTINATARIOS_POR_DEFECTO = ["crgonzalezh@gmail.com"]
UMBRAL_ENVIO_REPETICION = 3  #  umbral para modificar 

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
            servidor.login(REMITENTE, PASSWORD)
            servidor.sendmail(REMITENTE, destinatarios, msg.as_string())
            logger.info(f"[EMAIL] Correo enviado a {destinatarios}.")
    except Exception as e:
        logger.error(f"[EMAIL] Error al enviar correo: {e}")

def notificar_alerta(sensor_id, nombre_estacion, valor, contador):
    asunto = "⚠️ Alerta de Umbral Superado"
    cuerpo = (
        f"Se ha detectado un valor fuera de rango en el sensor {sensor_id} "
        f"de la estación {nombre_estacion}. Valor: {valor}.\n\n"
    )

    if contador == 1:
        cuerpo += "⚠️ Esta es la primera vez que se detecta esta alerta."
        enviar_correo(DESTINATARIOS_POR_DEFECTO, asunto, cuerpo)
    elif contador == UMBRAL_ENVIO_REPETICION:
        cuerpo += (
            f"⏳ La alerta ha persistido durante {UMBRAL_ENVIO_REPETICION} revisiones consecutivas."
        )
        enviar_correo(DESTINATARIOS_POR_DEFECTO, asunto, cuerpo)
