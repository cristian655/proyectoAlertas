# enviar_alertas_pendientes.py
import os
import pymysql
from dotenv import load_dotenv
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
import sys
import logging

# ---------------------------
# Configuración inicial
# ---------------------------

load_dotenv()

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# Configuración base de datos
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASSWORD")
DB_PORT = int(os.getenv("DB_PORT", 3306))

# Configuración SMTP
SMTP_SERVER = "email-smtp.us-east-1.amazonaws.com"
SMTP_PORT = 587
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")
REMITENTE = "info@gptelemetria.cl"

# Bases de datos y tablas
TABLAS_Y_BASES = [
    ("GP-MLP-Telemtry", "Alertas"),
    ("GP-MLP-Contac", "alertas")
]

# Modo de envío: 1 = solo oficina / 2 = solo cliente / 3 = ambos
MODO_ENVIO = int(os.getenv("MODO_ENVIO", 3))

DESTINATARIOS_OFICINA = [
    "cgonzalez@gpconsultores.cl"]#, "erivas@gpconsultores.cl",
    #"hjilberto@gpconsultores.cl", "rconstanzo@gpconsultores.cl"
#]
DESTINATARIOS_CLIENTE = []#"vvaldebenito@pelambres.cl"]

if MODO_ENVIO == 1:
    DESTINATARIOS = DESTINATARIOS_OFICINA
elif MODO_ENVIO == 2:
    DESTINATARIOS = DESTINATARIOS_CLIENTE
else:
    DESTINATARIOS = DESTINATARIOS_OFICINA + DESTINATARIOS_CLIENTE


# ---------------------------
# Función de envío autónoma
# ---------------------------

def enviar_correo_html_con_logo(destinatarios, asunto, cuerpo_html, path_logo):
    """
    Envío de correo HTML con logo (autónomo, sin depender de enviar_correo.py)
    """
    if not SMTP_USER or not SMTP_PASS:
        print("[ERROR] Credenciales SMTP no cargadas. Revisa tu archivo .env")
        return

    if not os.path.exists(path_logo):
        print(f"[ERROR] Logo no encontrado en la ruta: {path_logo}")
        return

    msg = MIMEMultipart("related")
    msg["From"] = REMITENTE
    msg["To"] = ", ".join(destinatarios)
    msg["Subject"] = asunto

    msg_alt = MIMEMultipart("alternative")
    msg.attach(msg_alt)

    html_con_logo = f"""
    <html>
      <body style="font-family: Arial, sans-serif; background-color: #f4f6f8; padding: 20px;">
        <div style="max-width:600px;margin:auto;background:#fff;padding:20px;border-radius:8px;">
          <div style="display:flex;align-items:center;margin-bottom:15px;">
            <img src="cid:logo_gp" width="100" alt="GP Consultores" style="margin-right:15px;" />
            <h2 style="color:#018ae4;margin:0;">Reporte Automático de Alertas</h2>
          </div>
          {cuerpo_html}
          <hr style="margin-top:30px;border:none;border-top:1px solid #eee;" />
          <p style="font-size:12px;color:#999;text-align:center;">
            Este correo fue generado automáticamente por el sistema de monitoreo.<br>
            <em>No responder a esta dirección.</em>
          </p>
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

        print(f"[OK] Correo enviado a {destinatarios}")
    except Exception as e:
        print(f"[ERROR] Fallo al enviar correo: {e}")


# ---------------------------
# Funciones auxiliares
# ---------------------------

def conectar_bd(base):
    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        database=base,
        port=DB_PORT,
        cursorclass=pymysql.cursors.DictCursor
    )


def obtener_nombre_estacion(conn, base, estacion_id):
    tabla = "Estaciones" if base == "GP-MLP-Telemtry" else "estaciones"
    campo = "nombre"
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT {campo} FROM {tabla} WHERE estacion_id = %s", (estacion_id,))
            resultado = cursor.fetchone()
            return resultado[campo] if resultado else f"ID {estacion_id}"
    except Exception as e:
        logger.error(f"[ERROR] al obtener nombre estación {estacion_id} en {base}: {e}")
        return f"ID {estacion_id}"


def obtener_tipo_sensor(conn, base, sensor_id):
    if base == "GP-MLP-Telemtry":
        tabla = "Sensores"
        campo = "tipo"
    else:
        tabla = "sensores"
        campo = "tipo_raw"

    try:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT {campo} FROM {tabla} WHERE sensor_id = %s", (sensor_id,))
            resultado = cursor.fetchone()
            return resultado[campo] if resultado and resultado[campo] else f"ID {sensor_id}"
    except Exception as e:
        logger.error(f"[ERROR] al obtener tipo sensor {sensor_id} en {base}: {e}")
        return f"ID {sensor_id}"


def obtener_unidad_sensor(conn, base, sensor_id):
    try:
        with conn.cursor() as cursor:
            if base == "GP-MLP-Telemtry":
                cursor.execute("""
                    SELECT u.unidad
                    FROM unidades u
                    JOIN Sensores s ON u.id_unidad = s.id_unidad
                    WHERE s.sensor_id = %s
                """, (sensor_id,))
            else:
                cursor.execute("""
                    SELECT unidad FROM sensores WHERE sensor_id = %s
                """, (sensor_id,))

            resultado = cursor.fetchone()
            return resultado["unidad"] if resultado and resultado["unidad"] else ""
    except Exception as e:
        logger.error(f"[ERROR] al obtener unidad de sensor {sensor_id} en {base}: {e}")
        return ""


def obtener_alertas_no_notificadas(conn, tabla, base):
    with conn.cursor() as cursor:
        campo_id = "alerta_id"
        campo_fecha = "timestamp"
        tabla_real = "Alertas" if base == "GP-MLP-Telemtry" else "alertas"

        cursor.execute(f'''
            SELECT {campo_id}, estacion_id, sensor_id, criterio_id, valor, observacion, {campo_fecha} AS fecha_hora
            FROM {tabla_real}
            WHERE criterio_id IN (1, 2)
              AND notificado = 0
              AND enable = 1
            ORDER BY {campo_fecha} DESC
        ''')
        return cursor.fetchall()


def construir_alertas_html(alertas_con_conns):
    if not alertas_con_conns:
        return "<p>No se encontraron alertas.</p>"

    bloques = ""
    for base, _, conn, a in alertas_con_conns:
        criterio = a["criterio_id"]
        tipo = "Falla de comunicación" if criterio == 1 else "Umbral"
        nombre_estacion = obtener_nombre_estacion(conn, base, a["estacion_id"])
        tipo_sensor = obtener_tipo_sensor(conn, base, a["sensor_id"])
        unidad = obtener_unidad_sensor(conn, base, a["sensor_id"])
        observacion = a.get("observacion", "-")
        fecha = a["fecha_hora"]
        valor = a["valor"] if a["valor"] is not None else "--"

        bloques += f"""
        <div style="margin-bottom: 20px; padding: 15px; border: 1px solid #eee; border-radius: 8px;">
            <p>⚠️ <strong>Alerta en el sensor {tipo_sensor}</strong></p>
            <p>Estación: <strong>{nombre_estacion}</strong></p>
            <p>Valor medido: <strong>{valor} {unidad}</strong></p>
            <p>Tipo: <strong>{tipo}</strong></p>
            <p>{observacion}</p>
            <p style="color:#555; font-size: 12px;">{fecha}</p>
        </div>
        """

    return f"""
    <h3 style="color:#018ae4;">Alertas Detectadas</h3>
    {bloques}
    <p style="font-size: 12px; color: #999;">Revisar el sistema para más detalles.</p>
    """


def marcar_alertas_como_notificadas(conn, tabla, base, ids):
    if not ids:
        return
    campo_id = "alerta_id"
    ids_str = ",".join(str(i) for i in ids)
    with conn.cursor() as cursor:
        cursor.execute(f'''
            UPDATE {tabla}
            SET notificado = 1
            WHERE {campo_id} IN ({ids_str})
        ''')
        conn.commit()


# ---------------------------
# Script principal
# ---------------------------

def main():
    todas_alertas = []
    conexiones = []

    print(f"SMTP_USER: {SMTP_USER}")
    print(f"SMTP_PASS: {'CARGADO' if SMTP_PASS else 'NO CARGADO'}")

    if not SMTP_USER or not SMTP_PASS:
        print("[ERROR] Las credenciales SMTP no están cargadas. Revisa tu archivo .env")
        sys.exit(1)

    if not os.path.exists("gp-fullcolor-centrado.png"):
        print("[ERROR] Logo gp-fullcolor-centrado.png no encontrado en el directorio actual.")
        sys.exit(1)

    try:
        for base, tabla in TABLAS_Y_BASES:
            conn = conectar_bd(base)
            conexiones.append((conn, base, tabla))
            alertas = obtener_alertas_no_notificadas(conn, tabla, base)

            print(f"[{base}] Alertas detectadas: {len(alertas)}")
            todas_alertas.extend([(base, tabla, conn, a) for a in alertas])

        if not todas_alertas:
            print("[ALERTAS] No se encontraron alertas pendientes.")
            return

        html = construir_alertas_html(todas_alertas)
        asunto = f"⚠️ Reporte Automático de Alertas [{len(todas_alertas)}]"
        print("[ENVÍO] Enviando correo...")
        enviar_correo_html_con_logo(DESTINATARIOS, asunto, html, "gp-fullcolor-centrado.png")
        print("[ENVÍO] Correo enviado")

        # Marcar como notificadas
        for conn, base, tabla in [(c[0], c[1], c[2]) for c in conexiones]:
            ids = [a[3]['alerta_id'] for a in todas_alertas if a[0] == base and a[1] == tabla]
            print(f"[MARCAR] Marcando como notificadas en {base}: {ids}")
            marcar_alertas_como_notificadas(conn, tabla, base, ids)

    except Exception as e:
        print(f"[ERROR] {e}")
    finally:
        for conn, _, _ in conexiones:
            conn.close()


if __name__ == "__main__":
    main()
