# test_conexion.py
from sqlalchemy import text
from conexion import engine
try:
	with engine.connect() as conn:
		result = conn.execute(text("SELECT 1"))
		print("Conexion exitosa:", result.scalar()) 
except Exception as e:
	print("Error al conectar:", e)
