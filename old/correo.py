import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def enviar_correo(destinatarios, asunto, cuerpo):
    remitente = "crgonzalezh@gmail.com"
    password = "cids nbjo nclr vfnt "

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

enviar_correo(
    destinatarios=["crgonzalezh@gmail.com"],#cgcorreoprueba25@gmail.com"],
    asunto="⚠️ Alerta de Umbral Superado",
    cuerpo="Se ha detectado un valor fuera de rango en el sensor 123, estación ABC.\n\nRevisar de inmediato."
)
