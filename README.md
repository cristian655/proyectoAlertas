# Proyecto Alertas

Este proyecto detecta eventos y dispara alertas a partir de datos de sensores usando Python, SQLAlchemy y una base de datos MySQL en AWS.

## Requisitos

- Python 3.8 o superior (se recomienda Python 3.10)
- pip
- Acceso a una base de datos compatible con MySQL (ej: AWS RDS)
- Archivo `.env` con credenciales (ver ejemplo abajo)

## Instalación

1. Clonar el repositorio:

```bash
git clone https://github.com/cristian655/proyectoAlertas.git
cd proyectoAlertas
```

2. Crear un entorno virtual (opcional pero recomendado):

```bash
python -m venv venv
source venv/bin/activate  # En Windows: .\venv\Scripts\activate
```

3. Instalar las dependencias:

```bash
pip install -r requirements.txt
```

4. Crear un archivo `.env` basado en `.env.example`:

```bash
cp .env.example .env  # En Windows: copy .env.example .env
```

Editar el contenido de `.env` con tus credenciales reales:

```ini
DB_USER=usuario
DB_PASSWORD=clave
DB_HOST=host
DB_PORT=3306
DB_NAME=nombre_de_base
```

## Estructura

- `conexion.example.py`: Ejemplo de conexión a la base de datos usando variables de entorno.
- `conexion.py`: Archivo real de conexión (excluido del repositorio).
- `.env`: Variables de entorno (excluido del repositorio).
- `test_conexion.py`: Script para probar la conexión.

## Prueba de conexión

Puedes probar si la conexión a la base de datos funciona correctamente ejecutando:

```bash
python test_conexion.py
```

Deberías ver:

```
Conexión exitosa: 1
```

## Notas

- `conexion.py` y `.env` están excluidos del repositorio por razones de seguridad.
- Si clonas este proyecto en otra máquina, recuerda crearlos a partir de los archivos `.example`.

## 🧠 Uso de Git y entorno virtual

> ¿Debo activar el entorno virtual para usar Git?  
> **No es necesario.** Git funciona de forma independiente al entorno virtual.

### ✅ Cuándo debes activar el entorno:

- Para ejecutar scripts (`python script.py`)
- Para instalar paquetes (`pip install`)
- Para probar conexiones o correr tareas del proyecto

### ❌ Cuándo **no** necesitas activarlo:

- Para hacer `git pull`, `git add`, `git commit`, `git push`
- Para revisar el estado (`git status`) o cambiar de rama

### 🗞 Tabla rápida

| Acción                      | ¿Requiere entorno activo? |
|-----------------------------|----------------------------|
| `git pull`, `git push`      | ❌ No                     |
| `python script.py`          | ✅ Sí                    |
| `pip install paquete`       | ✅ Sí                    |
| `git status`                | ❌ No                     |
| `python test_conexion.py`   | ✅ Sí                    |

