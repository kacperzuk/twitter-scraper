import psycopg2
import json
import time
import os

conn = psycopg2.connect("dbname=%s user=%s host=%s password=%s" % (
    os.getenv("PGDATABASE"),
    os.getenv("PGUSER"),
    os.getenv("PGHOST"),
    os.getenv("PGPASSWORD")))
cur = conn.cursor()

def get_result_async(tag):
    cur.execute("select id, result, metadata from res_queue where tag = %s and is_processing_since is null order by id limit 1 for update skip locked", (tag,))
    row = cur.fetchone()
    if row:
        cur.execute("update res_queue set is_processing_since = current_timestamp where id = %s", (row[0],))
        return { "id": row[0], "result": json.loads(row[1]), "metadata": json.loads(row[2]) }
    return None

def finish_result(res):
    cur.execute("delete from res_queue where id = %s", (res["id"],))

def get_or_wait_for_result(tag):
    res = get_result_async(tag)
    if res:
        return res

    while True:
        cur.execute("(select 1 from cmd_queue where tag = %(tag)s limit 1) union (select 1 from res_queue where is_processing_since = null and tag = %(tag)s limit 1)", { "tag": tag })
        if cur.fetchone():
            time.sleep(0.01)
            res = get_result_async(tag)
            if res:
                return res
        else:
            break

    return None

def command(method, path, params, tag, metadata=None):
    cur.execute("insert into cmd_queue (method, path, params, tag, metadata) values (%s, %s, %s, %s, %s)", (method, path, json.dumps(params), tag, json.dumps(metadata)))
