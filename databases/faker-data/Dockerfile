# Usa una imagen base de Python (puedes elegir la versión que prefieras)
FROM python:3.9-slim

# Actualiza pip y luego instala las librerías necesarias
RUN pip install --no-cache-dir psycopg2-binary

# Establece el directorio de trabajo
WORKDIR /app

# Copia el script de Python a la imagen
COPY app.py /app/app.py

# El contenedor ejecutará el script cuando inicie
CMD ["python", "app.py"]
