import psycopg2
import json
import pika
import time
import os

params = pika.URLParameters("amqp://127.0.0.1:32769")
connection = pika.BlockingConnection(params)
jobs = connection.channel() # start a channel
jobs.queue_declare(queue='jobs', durable=True) # Declare a queue
responses = connection.channel()

conn = psycopg2.connect("dbname=%s user=%s host=%s password=%s" % (
    os.getenv("PGDATABASE"),
    os.getenv("PGUSER"),
    os.getenv("PGHOST"),
    os.getenv("PGPASSWORD")))
cur = conn.cursor()

def command(method, path, params, tag, metadata=None):
    global jobs
    cmd = json.dumps({
        "method": method,
        "path": path,
        "params": params,
        "tag": tag,
        "metadata": metadata
    })
    jobs.basic_publish(exchange='',
            routing_key='jobs',
            body=cmd)

def get_response(tag):
    responses.queue_declare(queue='responses_'+tag, durable=True)
    while True:
        isok, properties, resp = responses.basic_get("responses_"+tag)
        if isok:
            return isok, json.loads(resp)
        time.sleep(0.01)

def ack_response(meta):
    responses.basic_ack(meta.delivery_tag)

def nack_response(meta):
    responses.basic_nack(meta.delivery_tag)
