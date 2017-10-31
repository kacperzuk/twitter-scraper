import psycopg2
import json
import pika
import time
import os

params = pika.URLParameters(os.getenv("AMQP_CONN_STRING"))
connection = pika.BlockingConnection(params)
jobs = connection.channel() # start a channel
agg = connection.channel() # start a channel
responses = connection.channel()

conn = psycopg2.connect("dbname=%s user=%s host=%s password=%s" % (
    os.getenv("PGDATABASE"),
    os.getenv("PGUSER"),
    os.getenv("PGHOST"),
    os.getenv("PGPASSWORD")))
cur = conn.cursor()

def s(string):
    return string.translate({ 0: None })

def command(method, path, params, tag, metadata=None):
    global jobs
    cmd = json.dumps({
        "method": method,
        "path": path,
        "params": params,
        "tag": tag,
        "metadata": metadata
    })
    responses.queue_declare(queue='responses_'+tag, durable=True, auto_delete=False)
    jobs.queue_declare(queue='jobs_'+tag, durable=True) # Declare a queue
    jobs.basic_publish(exchange='',
            routing_key='jobs_'+tag,
            body=cmd)

def aggregate(tag, param):
    n = agg.queue_declare(queue='agg_'+tag, durable=True).method.message_count
    agg.basic_publish(exchange='', routing_key='agg_'+tag, body=param)
    return n+1

def get_aggregate(tag, n):
    ret = []
    for i in range(n):
        isok, properties, resp = agg.basic_get("agg_"+tag)
        ret.append(resp.decode("utf-8"))
        agg.basic_ack(isok.delivery_tag)
    return ret

def get_response(tag):
    responses.queue_declare(queue='responses_'+tag, durable=True, auto_delete=False)
    while True:
        isok, properties, resp = responses.basic_get("responses_"+tag)
        if isok:
            return isok, json.loads(resp)
        time.sleep(0.01)

def ack_response(meta):
    responses.basic_ack(meta.delivery_tag)

def nack_response(meta):
    responses.basic_nack(meta.delivery_tag)
