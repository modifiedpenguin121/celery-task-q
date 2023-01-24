from celery import Celery
from kombu import Exchange, Queue
import time
import psycopg2
from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import datetime


def Log(customer_id_x,request_x,response_x):
    try:
        connection = psycopg2.connect(user="test1",
                                  password="guest",
                                  host="127.0.0.1",
                                  port="5432",
                                  database="mydb")
        cursor = connection.cursor()

        postgres_insert_query = """ INSERT INTO querylog(customerid, request, response,createdat) VALUES (%s,%s,%s,%s)"""
        record_to_insert = (customer_id_x, request_x, response_x, datetime.datetime.utcnow())
        cursor.execute(postgres_insert_query, record_to_insert)

        connection.commit()
        count = cursor.rowcount
        print(count, "Record inserted successfully into query log")

    except (Exception, psycopg2.Error) as error:
        print("Failed to insert record into program_logs table", error)

    finally:
        # closing database connection.
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")


app = Celery("app", broker="amqp://127.0.0.1:5672",backend="")

# configure the task exchange and queues
photo_exchange = Exchange("photos", type="topic")
video_exchange = Exchange("videos", type="topic")
app.conf.task_queues = (
    Queue("photo_queue", photo_exchange, routing_key="photo"),
    Queue("video_queue", video_exchange, routing_key="video"),
    #Queue("photo_queue_priority", photo_exchange, routing_key="priorityphoto.#"),
   #Queue("video_queue_priority", video_exchange, routing_key="priorityvideo.#"),
)
# app.conf.task_default_queue = "photo_queue"
# app.conf.task_default_exchange = "photos"
# app.conf.task_default_exchange_type = "topic"

# specify the routing key for each task
app.conf.task_routes = {
    "tasks.process_photos": {"queue": "photo_queue", "routing_key": "photo"},
    #"tasks.process_photos": {"queue": "photo_queue_priority", "routing_key": "priorityphoto"},
    "tasks.process_videos": {"queue": "video_queue", "routing_key": "video"},
    #"tasks.process_videos": {"queue": "video_queue_priority", "routing_key": "priorityvideo"},
}


@app.task(name='celery_tasks.tasks.process_photos')
def process_photos(file_path: str, customer_id: str):
    # simulate processing time
    time.sleep(10)
    response = f"Your photo at {file_path} is processed"
    Log(request_x="photo", response_x=response, customer_id_x=customer_id)
    
    return response


@app.task(name='celery_tasks.tasks.process_videos')
def process_videos(file_path: str, customer_id: str):
    # simulate processing time
    time.sleep(20)
    response = f"Your video at {file_path} is processed"
    Log(request_x="video", response_x=response, customer_id_x=customer_id)
   
    return response
