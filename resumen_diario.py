from alerta import obtener_resumen_diario
from enviar_correo import DESTINATARIOS_POR_DEFECTO
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
import os

def enviar_resumen_diario():
    df = obtener_resumen_diario()
    total_alertas = len(df)

    # Cuerpo HTML responsivo con logo y tabla
    cuerpo_html = f"""
    <html>
      <head>
        <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
        <style>
            table {{
                width: 100%;
                border-collapse: collapse;
                font-family: Arial, sans-serif;
                font-size: 14px;
            }}
            th, td {{
                border: 1px solid #dddddd;
                text-align: left;
                padding: 8px;
            }}
            th {{
                background-color: #f2f2f2;
            }}
            tr:nth-child(even) {{
                background-color: #f9f9f9;
            }}
        </style>
      </head>
      <body style="font-family: 'Segoe UI', Roboto, Arial, sans-serif; background-color: #f4f6f8; padding: 30px; color: #333;">
        <div style="max-width: 600px; margin: auto; background: #ffffff; padding: 25px; border-radius: 10px; box-shadow: 0 2px 8px rgba(0,0,0,0.05);">

          <!-- Encabezado con logo -->
          <div style="text-align: center; margin-bottom: 20px;">
            <img src="cid:logo_gp"
                 alt="GP Consultores"
                 width="100"
                 style="width:100px; max-width:100px; height:auto; display:block; margin:auto;" />
            <h2 style="color: #018ae4; margin-top: 10px;">📊 Resumen Periódico del Sistema de Alertas</h2>
            <p style="margin:0; color:#555;">Se encontraron <strong>{total_alertas}</strong> alertas activas.</p>
          </div>

          <div style="font-size: 15px; color: #333; margin: 15px 0; overflow-x:auto;">
            {df.to_html(index=False, escape=False) if not df.empty else "<p>No se encontraron alertas.</p>"}
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

    asunto = f"📊 Resumen Periódico del Sistema de Alertas ({total_alertas} alertas)"

    # Configuración SMTP
    from dotenv import load_dotenv
    load_dotenv()
    SMTP_SERVER = "email-smtp.us-east-1.amazonaws.com"
    SMTP_PORT = 587
    SMTP_USER = os.getenv("SMTP_USER")
    SMTP_PASS = os.getenv("SMTP_PASS")
    REMITENTE = "info@gptelemetria.cl"

    msg = MIMEMultipart("related")
    msg["From"] = REMITENTE
    msg["To"] = ", ".join(DESTINATARIOS_POR_DEFECTO)
    msg["Subject"] = asunto

    msg_alt = MIMEMultipart("alternative")
    msg.attach(msg_alt)
    msg_alt.attach(MIMEText(cuerpo_html, "html"))

    # Adjuntar el logo como inline para Outlook
    logo_path = os.path.join(os.path.dirname(__file__), "gp-fullcolor-centrado.png")
    with open(logo_path, "rb") as f:
        logo = MIMEImage(f.read())
        logo.add_header("Content-ID", "<logo_gp>")
        logo.add_header("Content-Disposition", "inline", filename="gp-fullcolor-centrado.png")
        msg.attach(logo)

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as servidor:
        servidor.starttls()
        servidor.login(SMTP_USER, SMTP_PASS)
        servidor.sendmail(REMITENTE, DESTINATARIOS_POR_DEFECTO, msg.as_string())


if __name__ == "__main__":
    enviar_resumen_diario()
