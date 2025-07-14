# enviar_alertas_pendientes.py

import os
import pymysql
from dotenv import load_dotenv
from datetime import datetime
from enviar_correo import enviar_correo_html_con_logo, logger, SMTP_USER, SMTP_PASS
import sys

# Cargar variables de entorno
load_dotenv()

# Configuración de base de datos
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASSWORD")
DB_PORT = int(os.getenv("DB_PORT", 3306))

# Bases de datos y tablas
TABLAS_Y_BASES = [
    ("GP-MLP-Telemtry", "Alertas"),
    ("GP-MLP-Contac", "alertas")
]

MODO_ENVIO = int(os.getenv("MODO_ENVIO", 1))

DESTINATARIOS_OFICINA = [
    "cgonzalez@gpconsultores.cl"
]

DESTINATARIOS_CLIENTE = [
    "cliente@externo.cl"
]

if MODO_ENVIO == 1:
    DESTINATARIOS = DESTINATARIOS_OFICINA
elif MODO_ENVIO == 2:
    DESTINATARIOS = DESTINATARIOS_CLIENTE
else:
    DESTINATARIOS = DESTINATARIOS_OFICINA + DESTINATARIOS_CLIENTE

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
    tabla = "Sensores" if base == "GP-MLP-Telemtry" else "sensores"
    campo = "tipo"
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT {campo} FROM {tabla} WHERE sensor_id = %s", (sensor_id,))
            resultado = cursor.fetchone()
            return resultado[campo] if resultado and resultado[campo] else f"ID {sensor_id}"
    except Exception as e:
        logger.error(f"[ERROR] al obtener tipo sensor {sensor_id} en {base}: {e}")
        return f"ID {sensor_id}"

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
        alertas = cursor.fetchall()
        for alerta in alertas:
            logger.info(f"[DEBUG] Alerta encontrada en {base}: {alerta}")
        return alertas

def construir_tabla_html(alertas_con_conns):
    filas = ""
    for base, _, conn, a in alertas_con_conns:
        try:
            tipo = "Umbral" if a["criterio_id"] == 1 else "Detención"
            nombre_estacion = obtener_nombre_estacion(conn, base, a["estacion_id"])
            tipo_sensor = obtener_tipo_sensor(conn, base, a["sensor_id"])
            observacion = a.get("observacion", "-")
        except Exception as e:
            logger.error(f"[ERROR] Falló lookup en {base} para alerta {a['alerta_id']}: {e}")
            nombre_estacion = f"ID {a['estacion_id']}"
            tipo_sensor = f"ID {a['sensor_id']}"
            observacion = "Error"

        filas += f"<tr><td>{a['fecha_hora']}</td><td>{nombre_estacion}</td><td>{tipo_sensor}</td><td>{a['valor']}</td><td>{tipo}</td><td>{observacion}</td></tr>"

    html = f"""
    <h3>Alertas Detectadas</h3>
    <table border="1" cellspacing="0" cellpadding="4" style="border-collapse: collapse; width: 100%;">
        <tr style="background-color: #005b5e; color: white;">
            <th>Fecha</th><th>Estación</th><th>Sensor</th><th>Valor</th><th>Tipo</th><th>Observación</th>
        </tr>
        {filas}
    </table>
    <p style="font-size: 14px;">Revisar el sistema para más detalles.</p>
    """
    return html

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
            for a in alertas:
                print(a)

            todas_alertas.extend([(base, tabla, conn, a) for a in alertas])

        if not todas_alertas:
            print("[ALERTAS] No se encontraron alertas pendientes.")
            return

        html = construir_tabla_html(todas_alertas)
        asunto = f"{len(todas_alertas)} nuevas alertas generadas ({datetime.now().strftime('%d-%m-%Y %H:%M')})"
        print("[ENVÍO] Enviando correo...")
        enviar_correo_html_con_logo(DESTINATARIOS, asunto, html, "gp-fullcolor-centrado.png")
        print("[ENVÍO] Correo enviado")

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
