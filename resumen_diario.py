from alerta import obtener_resumen_diario
from enviar_correo import enviar_correo, DESTINATARIOS_POR_DEFECTO

def enviar_resumen_diario():
    df = obtener_resumen_diario()
    if df.empty:
        cuerpo = "No se encontraron datos en la vista de resumen."
    else:
        cuerpo = df.to_string(index=False)

    asunto = "📊 Resumen Diario del Sistema de Alertas"
    enviar_correo(DESTINATARIOS_POR_DEFECTO, asunto, cuerpo)

if __name__ == "__main__":
    enviar_resumen_diario()
