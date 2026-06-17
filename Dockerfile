# Usamos una imagen ligera de Python
FROM python:3.11-slim

# Instalamos dependencias del sistema necesarias para psycopg2
RUN apt-get update && apt-get install -y libpq-dev gcc && rm -rf /var/lib/apt/lists/*

# Definimos el directorio de trabajo
WORKDIR /app

# Instalamos las librerías de Python
RUN pip install psycopg2-binary

# Copiamos todo el proyecto al contenedor
COPY . .

# Comando para correr el script (esto es solo para testear)
CMD ["python", "python/main.py"]