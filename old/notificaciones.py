# notificaciones.py

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def enviar_correo(destinatarios, asunto, cuerpo, remitente="crgonzalezh@gmail.com", password="tu_clave"):
    msg = MIMEMultipart()
    msg["From"] = remitente
    msg["To"] = ", ".join(destinatarios)
    msg["Subject"] = asunto

    msg.attach(MIMEText(cuerpo, "plain"))

    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as servidor:
            servidor.starttls()
            servidor.login(remitente, password)
            servidor.sendmail(remitente, destinatarios, msg.as_string())
            print("[INFO] Correo enviado.")
    except Exception as e:
        print(f"[ERROR] Falló el envío de correo: {e}")
