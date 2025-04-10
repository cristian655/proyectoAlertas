from sqlalchemy import create_engine, text

# Configuración de conexión
DB_CONFIG = {
    "user": "root",
    "password": "1234",
    "host": "127.0.0.1",
    "port": "3306",
    "database": "gp-mlp-telemtry"
}

DATABASE_URL = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
engine = create_engine(DATABASE_URL)

# Ejecutar procedimiento almacenado
def actualizar_ultimo_dato_sensor():
    try:
        with engine.begin() as conn:
            conn.execute(text("CALL actualizar_ultimo_dato_sensor()"))
        print("[INFO] Tabla 'ultimo_dato_sensor' actualizada correctamente.")
    except Exception as e:
        print(f"[ERROR] al ejecutar procedimiento: {e}")

if __name__ == "__main__":
    actualizar_ultimo_dato_sensor()
