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
    cur.execute("select 1 from cmd_queue where tag = %s limit 1", (tag,))
    conn.commit()
    return cur.fetchone()

def results_in_queue(tag):
    cur.execute("select 1 from res_queue where tag = %s limit 1", (tag,))
    conn.commit()
    return cur.fetchone()

uid_cache = []
def insert_tweet(tweet):
    global uid_cache
    retweet_of = None
    if "retweeted_status" in tweet and tweet["retweeted_status"]:
        insert_tweet(tweet["retweeted_status"])
        retweet_of = tweet["retweeted_status"]["id_str"]
    uid = tweet["user"]["id_str"]
    if uid not in uid_cache:
        cur.execute("insert into metadata (uid, nest_level) values (%s, 2147483647) on conflict do nothing", (uid,))
        uid_cache.append(uid)
    try:
        cur.execute("""
        insert into tweets (
        twid,
        uid,
        tweet,
        created_at,
        truncated,
        hashtags,
        symbols,
        user_mentions,
        urls,
        in_reply_to_status_id,
        in_reply_to_user_id,
        in_reply_to_screen_name,
        geo,
        coordinates,
        place,
        retweet_of,
        is_quote_status,
        retweet_count,
        favorite_count,
        possibly_sensitive,
        lang
        ) values ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            tweet["id_str"],
            uid,
            tweet["text"],
            tweet["created_at"],
            tweet["truncated"],
            json.dumps(tweet["entities"]["hashtags"]),
            json.dumps(tweet["entities"]["symbols"]),
            json.dumps(tweet["entities"]["user_mentions"]),
            json.dumps(tweet["entities"]["urls"]),
            tweet["in_reply_to_status_id"],
            tweet["in_reply_to_user_id"],
            tweet["in_reply_to_screen_name"],
            tweet["geo"],
            tweet["coordinates"],
            tweet["place"],
            retweet_of,
            tweet["is_quote_status"],
            tweet["retweet_count"],
            tweet["favorite_count"],
            tweet["possibly_sensitive"],
            tweet["lang"]
        ))
    except:
        from pprint import pprint
        pprint(tweet)
        raise

def loop():
    cur2 = conn.cursor()
    cur2.execute("select uid from metadata where tweets_requested is null for update")
    cur.execute("update metadata set tweets_requested = current_timestamp where tweets_requested is null")
    conn.commit()
    print("Scheduling downloads for users...")
    i = 0
    for row in cur2:
        u = row[0]
        print(".", end="")
        sys.stdout.flush()
        command("get", "statuses/user_timeline",
                { "user_id": str(u), "trim_user": True, "count": 200, "include_rts": True, "exclude_replies": False },
                "tweets",
                { "user_id": u })
        i = i + 1
        if i % 1000 == 0:
            conn.commit()
    conn.commit()

    print("Processing tweets...")
    i = 0
    while True:
        print(".", end="")
        sys.stdout.flush()
        res = get_result_async("tweets")
        if not res:
            print("")
            if commands_in_queue("tweets") or results_in_queue("tweets"):
                res = get_result_sync("tweets")
            else:
                break
        if isinstance(res["result"], list) and len(res["result"]) > 0 and "id_str" not in res["result"][0]:
            cur.execute("update metadata set tweets_fetch_success = 'f' where uid = %s", (res["metadata"]["user_id"],))
        elif isinstance(res["result"], list):
            cur.execute("update metadata set tweets_fetch_success = 't' where uid = %s", (res["metadata"]["user_id"],))
            for tweet in res["result"]:
                insert_tweet(tweet)
                i = i + 1
                if i % 1000 == 0:
                    conn.commit()
        else:
            with open("log", "w") as f:
                f.write("Oddity found for tweets response: "+json.dumps(res))
        conn.commit()

    print("Done.")
    cur.execute("select count(*) from tweets")
    conn.commit()
    print("Total tweets in DB: %d" % (cur.fetchone()[0],))


loop()
