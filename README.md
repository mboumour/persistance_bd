# TP - Outils pour la Data

```bash
echo -e "AIRFLOW_UID=$(id -u)" > .env
chown -R $(whoami) ./airflow/*

docker compose pull
docker compose up -d --build
```

Plusieurs urls : 
* Airflow : http://localhost:8080 avec identifiants airflow/airflow
* MinIO : http://localhost:9000 avec identifiants minioadmin/minioadmin
* RabbitMQ : http://localhost:15672 avec identifiants guest/guest

sudo docker stop $(sudo docker ps -q)
sudo docker rmi -f $(sudo docker images -q)
sudo docker compose rm

sudo docker compose logs rabbitmq-to-minio


docker exec -it mongodb-container bash
sudo service mongod stop
sudo service rabbitmq-server stop


mongosh