# enviar_correo.py
import os
from datetime import datetime
from dotenv import load_dotenv
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from logger import logger

load_dotenv()

ENVIAR_CORREO = True

REMITENTE = "info@gptelemetria.cl"
SMTP_SERVER = "email-smtp.us-east-1.amazonaws.com"
SMTP_PORT = 587
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")

DESTINATARIOS_POR_DEFECTO = [
    "cgonzalez@gpconsultores.cl"#,
    #"erivas@gpconsultores.cl",
    #"hjilberto@gpconsultores.cl"
]
DESTINATARIOS_VICTOR = DESTINATARIOS_POR_DEFECTO
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


def enviar_correo_html_con_logo(destinatarios, asunto, cuerpo_html, path_logo):
    if not ENVIAR_CORREO:
        logger.info("[EMAIL] Envío de correos deshabilitado por configuración.")
        return

    if not SMTP_USER or not SMTP_PASS:
        logger.error("[EMAIL] Credenciales SMTP no cargadas correctamente (SMTP_USER o SMTP_PASS vacío).")
        return

    if not os.path.exists(path_logo):
        logger.error(f"[EMAIL] No se encontró el archivo de logo en la ruta: {path_logo}")
        return

    msg = MIMEMultipart("related")
    msg["From"] = REMITENTE
    msg["To"] = ", ".join(destinatarios)
    msg["Subject"] = asunto

    msg_alt = MIMEMultipart("alternative")
    msg.attach(msg_alt)

    # Plantilla responsive con logo proporcional
    html_con_logo = f"""
    <html>
      <head>
        <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
      </head>
      <body style="font-family: 'Segoe UI', Roboto, Arial, sans-serif; background-color: #f4f6f8; padding: 30px; color: #333;">
        <div style="max-width: 600px; margin: auto; background: #ffffff; padding: 25px; border-radius: 10px; box-shadow: 0 2px 8px rgba(0,0,0,0.05);">

          <!-- Encabezado con logo -->
          <div style="display: flex; align-items: center; margin-bottom: 20px;">
            <img src="cid:logo_gp"
                 alt="GP Consultores"
                 width="100"
                 style="width:100px; max-width:100px; height:auto; display:block; margin-right:20px;" />
            <h2 style="color: #018ae4; margin: 0; font-size: 24px;">Alerta del Sistema de Monitoreo</h2>
          </div>

          <div style="font-size: 14px; color: #666; margin-top: 10px;">
            Se han identificado condiciones que requieren revisión. A continuación se detallan los registros asociados:
          </div>

          <div style="font-size: 15px; color: #333; margin: 15px 0;">
            {cuerpo_html}
          </div>

          <hr style="margin: 30px 0; border: none; border-top: 1px solid #eee;" />

          <div style="font-size: 12px; color: #999; text-align: center;">
            Este correo fue generado automáticamente por el sistema de monitoreo de <strong>GP Consultores</strong>.<br>
            <em>Por favor, no respondas a esta dirección de correo electrónico.</em>
          </div>
        </div>
      </body>
    </html>
    """

    msg_alt.attach(MIMEText(html_con_logo, "html"))

    try:
        with open(path_logo, "rb") as f:
            logo = MIMEImage(f.read())
            logo.add_header("Content-ID", "<logo_gp>")
            msg.attach(logo)

        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as servidor:
            servidor.starttls()
            servidor.login(SMTP_USER, SMTP_PASS)
            servidor.sendmail(REMITENTE, destinatarios, msg.as_string())
            logger.info(f"[EMAIL] Correo HTML con logo enviado a {destinatarios}.")
    except Exception as e:
        logger.error(f"[EMAIL] Error al enviar correo con logo: {e}")


def notificar_alerta(tipo_sensor, nombre_estacion, valor, contador, fecha_hora):
    asunto = "⚠️ Alerta de Umbral Superado"

    cuerpo_html = f"""
    <p>⚠️ <strong>Alerta en el sensor {tipo_sensor}</strong></p>
    <p>Estación: <strong>{nombre_estacion}</strong></p>
    <p>Valor medido: <strong>{valor}</strong></p>
    """

    if contador == 1:
        cuerpo_html += f"<p>🔔 Primera detección de la alerta.<br><strong>{fecha_hora}</strong></p>"
    elif contador == UMBRAL_ENVIO_REPETICION:
        cuerpo_html += f"<p>⏳ La alerta ha persistido durante {UMBRAL_ENVIO_REPETICION} revisiones consecutivas.<br><strong>{fecha_hora}</strong></p>"
    else:
        return  # No enviar si no es ni la primera ni el umbral de repetición

    enviar_correo_html_con_logo(DESTINATARIOS_VICTOR, asunto, cuerpo_html, "gp-fullcolor-centrado.png")


def notificar_alerta_modelo(nombre_estacion, tipo_sensor, valor, fecha_hora, algoritmos_detectores):
    asunto = "⚠️ Alerta de anomalía detectada por tendencia"
    cuerpo = (
        f"Se ha detectado una anomalía en el sensor {tipo_sensor} "
        f"de la estación {nombre_estacion} mediante algoritmos de detección.\n\n"
        f" Algoritmos que detectaron la anomalía: {', '.join(algoritmos_detectores)}\n\n"
        f" Valor detectado: {valor}\n"
        f"Fecha y hora de la medición: {fecha_hora}\n\n"
        f"⚠️ Esta alerta fue generada por análisis estadístico/modelos."
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


def probar_alerta_umbral_con_logo():
    tipo_sensor = "pH"
    nombre_estacion = "CRW-01"
    valor = 9.8
    contador = 1
    fecha_hora = datetime.now().strftime("%d-%m-%Y %H:%M")
    notificar_alerta(tipo_sensor, nombre_estacion, valor, contador, fecha_hora)


if __name__ == "__main__":
    probar_alerta_umbral_con_logo()
