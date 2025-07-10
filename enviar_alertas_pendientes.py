# enviar_alertas_pendientes.py

import os
import pymysql
from dotenv import load_dotenv
from datetime import datetime
from enviar_correo import enviar_correo_html_con_logo, logger

# Cargar variables de entorno
load_dotenv()

# Configuración de base de datos
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_PORT = int(os.getenv("DB_PORT", 3306))

# Bases de datos y tablas
TABLAS_Y_BASES = [
    ("GP-MLP-Telemtry", "Alertas"),
    ("GP-MLP-Contac", "alertas")
]

# MODO_ENVIO:
# 1 = solo oficina
# 2 = solo cliente
# 3 = todos
MODO_ENVIO = int(os.getenv("MODO_ENVIO", 1))

DESTINATARIOS_OFICINA = [
    "cgonzalez@gpconsultores.cl",
    "erivas@gpconsultores.cl",
    "hjilberto@gpconsultores.cl"
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

def obtener_alertas_no_notificadas(conn, tabla):
    with conn.cursor() as cursor:
        cursor.execute(f'''
            SELECT alerta_id AS id, estacion_id, sensor_id, criterio_id, valor, timestamp AS fecha_hora
            FROM {tabla}
            WHERE criterio_id IN (1, 2)
              AND notificado = 0
              AND enable = 1
            ORDER BY timestamp DESC
        ''')
        alertas = cursor.fetchall()
        for alerta in alertas:
            logger.info(f"[DEBUG] Alerta encontrada: {alerta}")
        return alertas

def construir_tabla_html(alertas):
    filas = ""
    for a in alertas:
        tipo = "Umbral" if a["criterio_id"] == 1 else "Detención"
        filas += f"<tr><td>{a['fecha_hora']}</td><td>{a['estacion_id']}</td><td>{a['sensor_id']}</td><td>{a['valor']}</td><td>{tipo}</td></tr>"

    html = f"""
    <html>
    <body>
        <h3>Alertas Detectadas</h3>
        <table border="1" cellspacing="0" cellpadding="4">
            <tr><th>Fecha</th><th>Estación</th><th>Sensor</th><th>Valor</th><th>Tipo</th></tr>
            {filas}
        </table>
        <p>Revisar el sistema para más detalles.</p>
    </body>
    </html>
    """
    return html

def marcar_alertas_como_notificadas(conn, tabla, ids):
    if not ids:
        logger.info("[AVISO] No hay alertas para marcar como notificadas.")
        return
    ids_str = ",".join(str(i) for i in ids)
    with conn.cursor() as cursor:
        cursor.execute(f'''
            UPDATE {tabla}
            SET notificado = 1
            WHERE id IN ({ids_str})
        ''')
        conn.commit()
        logger.info(f"[BD] {len(ids)} alertas marcadas como notificadas en {tabla}")

def main():
    todas_alertas = []
    conexiones = []

    try:
        for base, tabla in TABLAS_Y_BASES:
            logger.info(f"[INFO] Revisando base: {base}, tabla: {tabla}")
            conn = conectar_bd(base)
            conexiones.append((conn, base, tabla))
            alertas = obtener_alertas_no_notificadas(conn, tabla)
            logger.info(f"[{base}] Alertas encontradas: {len(alertas)}")
            todas_alertas.extend([(base, tabla, conn, a) for a in alertas])

        if not todas_alertas:
            logger.info("[ALERTAS] No se encontraron alertas pendientes.")
            return

        html = construir_tabla_html([a[3] for a in todas_alertas])
        asunto = f"{len(todas_alertas)} nuevas alertas generadas ({datetime.now().strftime('%d-%m-%Y %H:%M')})"
        logger.info(f"[EMAIL] Enviando correo a: {DESTINATARIOS}")
        enviar_correo_html_con_logo(DESTINATARIOS, asunto, html, "gp-fullcolor-centrado.png")
        logger.info("[EMAIL] Correo enviado correctamente")

        for conn, base, tabla in [(c[0], c[1], c[2]) for c in conexiones]:
            ids = [a[3]['id'] for a in todas_alertas if a[0] == base and a[1] == tabla]
            marcar_alertas_como_notificadas(conn, tabla, ids)

    except Exception as e:
        logger.error(f"[ERROR GENERAL] {e}")
    finally:
        for conn, _, _ in conexiones:
            conn.close()
        logger.info("[INFO] Conexiones cerradas.")

if __name__ == "__main__":
    main()
