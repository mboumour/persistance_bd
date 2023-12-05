# Utilisez une image Python officielle
FROM python:3.8-slim

# Définir le répertoire de travail
WORKDIR /app

# Copier les fichiers nécessaires
COPY ./src/1_rabbit_to_minio.py /app/
COPY ./airflow/data/movies-stackexchange/json/Posts.json /app/

# Installer les dépendances Python
RUN pip install minio pika

# Définir la commande d'entrée
ENTRYPOINT ["python", "1_rabbit_to_minio.py"]
