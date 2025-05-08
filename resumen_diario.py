from alerta import obtener_resumen_diario
from enviar_correo import enviar_correo_html, DESTINATARIOS_POR_DEFECTO

def enviar_resumen_diario():
    df = obtener_resumen_diario()
    
    if df.empty:
        cuerpo = "<p>No se encontraron datos en la vista de resumen.</p>"
    else:
        cuerpo = f"""
        <html>
        <head>
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
        <body>
        <h3>Resumen Diario del Sistema de Alertas</h3>
        {df.to_html(index=False, escape=False)}
        </body>
        </html>
        """

    asunto = "📊 Resumen Diario del Sistema de Alertas"
    enviar_correo_html(DESTINATARIOS_POR_DEFECTO, asunto, cuerpo)

if __name__ == "__main__":
    enviar_resumen_diario()
