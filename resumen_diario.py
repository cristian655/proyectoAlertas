from alerta import obtener_resumen_diario
from enviar_correo import enviar_correo_html_con_logo, DESTINATARIOS_POR_DEFECTO

def enviar_resumen_diario():
    df = obtener_resumen_diario()
    total_alertas = len(df)

    if df.empty:
        cuerpo_html = """
        <html>
        <body style="font-family: Arial, sans-serif; background-color: #f4f6f8; padding: 30px; color: #333;">
        <div style="max-width: 800px; margin: auto; background: #ffffff; padding: 25px; border-radius: 10px; box-shadow: 0 2px 8px rgba(0,0,0,0.05);">
            <div style="display:flex; align-items:center; margin-bottom:15px;">
                <img src="cid:logo_gp" alt="GP Consultores" style="max-width: 100px; margin-right: 20px;" />
                <h2 style="color:#018ae4;">📊 Resumen Periódico del Sistema de Alertas</h2>
            </div>
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
            body {{
                font-family: 'Segoe UI', Roboto, Arial, sans-serif;
                background-color: #f4f6f8;
                padding: 30px;
                color: #333;
            }}
            .contenedor {{
                max-width: 800px;
                margin: auto;
                background: #ffffff;
                padding: 25px;
                border-radius: 10px;
                box-shadow: 0 2px 8px rgba(0,0,0,0.05);
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                font-size: 14px;
            }}
            th, td {{
                border: 1px solid #dddddd;
                text-align: left;
                padding: 8px;
            }}
            th {{
                background-color: #018ae4;
                color: white;
            }}
            tr:nth-child(even) {{
                background-color: #f9f9f9;
            }}
            .titulo {{
                color: #018ae4;
                margin-bottom: 10px;
            }}
        </style>
        </head>
        <body>
        <div class="contenedor">
            <div style="display:flex; align-items:center; margin-bottom:15px;">
                <img src="cid:logo_gp" alt="GP Consultores" style="max-width: 100px; margin-right: 20px;" />
                <div>
                    <h2 class="titulo">📊 Resumen Periódico del Sistema de Alertas</h2>
                    <p style="margin:0; color:#555;">Se encontraron <strong>{total_alertas}</strong> alertas activas.</p>
                </div>
            </div>
            {df.to_html(index=False, escape=False)}
            <p style="font-size:12px; color:#888; margin-top:20px;">
                Este correo fue generado automáticamente por el sistema de monitoreo de <strong>GP Consultores</strong>.
            </p>
        </div>
        </body>
        </html>
        """

    asunto = f"📊 Resumen Periódico del Sistema de Alertas ({total_alertas} alertas)"
    enviar_correo_html_con_logo(DESTINATARIOS_POR_DEFECTO, asunto, cuerpo_html, "gp-fullcolor-centrado.png")


if __name__ == "__main__":
    enviar_resumen_diario()
