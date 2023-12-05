import pika
from minio import Minio
import json
import os
import time

client = Minio(
    endpoint="minio:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

def safe_connect_rabbitmq():
    channel = None
    while not channel:
        try:
            connection = pika.BlockingConnection(pika.URLParameters("amqp://rabbitmq"))
            channel = connection.channel()
        except pika.exceptions.AMQPConnectionError:
            time.sleep(1)
    return channel

def callback(ch, method, properties, body):
    data_string = body.decode("utf-8")
    if not client.bucket_exists("posts"):
        client.make_bucket('posts')

    unique_id = time.time()*1000
    filepath = f"/tmp/{unique_id}.json"
    with open(filepath, "w") as f:
        f.write(data_string)
    client.fput_object("posts", os.path.basename(filepath), filepath)
    ch.basic_ack(delivery_tag=method.delivery_tag)
    os.remove(filepath)

    print(f"Uploaded message with id {unique_id}")

def main():
    print("Starting rabbit_to_minio.py")

    channel = safe_connect_rabbitmq()

    channel.queue_declare(queue='posts_to_minio')
    channel.basic_consume(
        queue='posts_to_minio',
        on_message_callback=callback
    )
    channel.start_consuming()
    print("Done")

if __name__ == "__main__":
    main()