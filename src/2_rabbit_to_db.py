import pika
from pymongo import MongoClient
import json

# Configure logging
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("mongodb_script.log"),
        logging.StreamHandler()
    ]
)

# Connexion à MongoDB
mongo_client = MongoClient('mongodb://mongodb:27017/')

# Création de la base de données
db = mongo_client['test']  # Remplacez 'nom_de_ma_base_de_donnees' par le nom souhaité

# Exemple : Créer une collection (facultatif)
collection = db['posts']  # Remplacez 'nom_de_ma_collection' par le nom souhaité

def callback(ch, method, properties, body):
    try:
        post = json.loads(body.decode('utf-8'))

        if "@Id" in post:
            existing_post = collection.find_one({"@Id": post["@Id"]})

            if not existing_post:
                collection.insert_one(post)
                logging.info(f"Post inserted into MongoDB: {post}")
            else:
                logging.info(f"The post already exists in MongoDB, Id: {post['@Id']}")
        else:
            logging.warning("The 'Id' field is absent in the post. The post will not be inserted into MongoDB.")

    except json.JSONDecodeError as e:
        logging.error(f"Error during JSON decoding: {e}")

def main():
    logging.info("Starting RabbitMQ to MongoDB")

    connection = pika.BlockingConnection(pika.URLParameters("amqp://rabbitmq"))
    channel = connection.channel()

    channel.queue_declare(queue='posts_to_redis')
    channel.basic_consume(queue='posts_to_redis', on_message_callback=callback, auto_ack=True)

    logging.info('Waiting for messages. To stop, press CTRL+C')
    channel.start_consuming()

if __name__ == "__main__":
    main()
