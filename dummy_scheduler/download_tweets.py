import psycopg2
import time
import json
import os
import sys

conn = psycopg2.connect("dbname=%s user=%s host=%s password=%s" % (
    os.getenv("PGDATABASE"),
    os.getenv("PGUSER"),
    os.getenv("PGHOST"),
    os.getenv("PGPASSWORD")))

cur = conn.cursor()
cur.execute("create table if not exists tweets ( twid varchar(100) primary key, uid varchar(100) references users(uid) not null, tweet text not null, created_at timestamp not null, error_fetching_tweets boolean)")
conn.commit()

def get_result_async(tag):
    cur.execute("select id, result, metadata from res_queue where tag = %s order by id for update skip locked limit 1", (tag,))
    row = cur.fetchone()
    if row:
        cur.execute("delete from res_queue where id = %s", (row[0],))
    conn.commit()
    if row:
        return { "result": row[1], "metadata": row[2] }
    return None

def get_result_sync(tag):
    print("Waiting for res tag=%s" % tag)
    result = None
    while not result:
        result = get_result_async(tag)
        if not result:
            time.sleep(0.1)
    return result

def command(method, path, params, tag, metadata=None):
    cur.execute("insert into cmd_queue (method, path, params, tag, metadata) values (%s, %s, %s, %s, %s)", (method, path, json.dumps(params), tag, json.dumps(metadata)))
    conn.commit()

def commands_in_queue(tag):
    cur.execute("select count(*) from cmd_queue where tag = %s", (tag,))
    conn.commit()
    return cur.fetchone()[0]

def results_in_queue(tag):
    cur.execute("select count(*) from res_queue where tag = %s", (tag,))
    conn.commit()
    return cur.fetchone()[0]

def loop():
    cur2 = conn.cursor()
    cur2.execute("select uid from users where tweets_fetched is null for update")
    cur.execute("update users set tweets_fetched = current_timestamp where tweets_fetched is null")
    conn.commit()
    print("Scheduling downloads for users...")
    i = 0
    for row in cur2:
        i = i + 1
        u = row[0]
        print(".", end="")
        command("get", "statuses/user_timeline",
                { "user_id": str(u), "trim_user": True },
                "tweets",
                { "user_id": u })
        if i % 100 == 0:
            conn.commit()
    conn.commit()

    print("Processing tweets...")
    while commands_in_queue("tweets") > 0 or results_in_queue("tweets") > 0:
        res = get_result_sync("tweets")
        for tweet in res["result"]:
            if "id_str" in tweet:
                twid = tweet["id_str"]
                uid = tweet["user"]["id_str"]
                text = tweet["text"]
                created_at = tweet["created_at"]
                cur.execute("insert into tweets (twid, uid, tweet, created_at) values (%s, %s, %s, %s)", (twid, uid, text, created_at))
                cur.execute("update users set error_fetching_tweets = 'f' where uid = %s", (uid,))
            else:
                cur.execute("update users set error_fetching_tweets = 't' where uid = %s", (uid,))
        conn.commit()


    print("Done.")
    cur.execute("select count(*) from tweets")
    conn.commit()
    print("Total tweets in DB: %d" % (cur.fetchone()[0],))


loop()
