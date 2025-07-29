from alerta import obtener_resumen_diario
from enviar_correo import enviar_correo_html, DESTINATARIOS_POR_DEFECTO

def enviar_resumen_diario():
    df = obtener_resumen_diario()
    total_alertas = len(df)

    if df.empty:
        cuerpo_html = """
        <html>
        <body style="font-family: Arial, sans-serif;">
            <div style="max-width: 800px; margin: auto; text-align: center;">
                <img src="cid:logo_gp" alt="GP Consultores" style="max-width: 120px; margin-bottom: 20px;" />
                <h2 style="color:#018ae4;">📊 Resumen Periódico del Sistema de Alertas</h2>
                <p>No se encontraron alertas.</p>
            </div>
        </body>
        </html>
        """
    else:
        cuerpo_html = f"""
        <html>
        <head>
        <style>
            table {{
                width: 800px;
                border-collapse: collapse;
                font-family: Arial, sans-serif;
                font-size: 14px;
            }}
            th, td {{
                border: 1px solid #dddddd;
                text-align: left;
                padding: 8px;
                vertical-align: top;
                word-wrap: break-word;
            }}
            th {{
                background-color: #f2f2f2;
            }}
            tr:nth-child(even) {{
                background-color: #f9f9f9;
            }}
        </style>
        </head>
        <body style="font-family: Arial, sans-serif;">
            <div style="max-width: 800px; margin: auto;">
                <div style="text-align: center; margin-bottom: 20px;">
                    <img src="cid:logo_gp" alt="GP Consultores" style="max-width: 120px; margin-bottom: 10px;" />
                    <h2 style="color:#018ae4;">📊 Resumen Periódico del Sistema de Alertas</h2>
                    <p style="margin:0; color:#555;">Se encontraron <strong>{total_alertas}</strong> alertas activas.</p>
                </div>
                {df.to_html(index=False, escape=False)}
                <p style="font-size:12px; color:#888; text-align:center; margin-top:20px;">
                    Este correo fue generado automáticamente por el sistema de monitoreo de <strong>GP Consultores</strong>.
                </p>
            </div>
        </body>
        </html>
        """

    asunto = f"📊 Resumen Periódico del Sistema de Alertas ({total_alertas} alertas)"
    # Solo un logo en el HTML, no adjuntamos automáticamente otro
    enviar_correo_html(DESTINATARIOS_POR_DEFECTO, asunto, cuerpo_html)


if __name__ == "__main__":
    enviar_resumen_diario()
