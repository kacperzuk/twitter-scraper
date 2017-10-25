import time
import json
import sys
from common import conn, cur, get_or_wait_for_result, command, finish_result

uid_cache = []
def insert_tweet(tweet):
    global uid_cache
    retweet_of = None
    if "retweeted_status" in tweet and tweet["retweeted_status"]:
        insert_tweet(tweet["retweeted_status"])
        retweet_of = tweet["retweeted_status"]["id_str"]
    quote_of = None
    if "quoted_status" in tweet and tweet["quoted_status"]:
        insert_tweet(tweet["quoted_status"])
        quote_of = tweet["quoted_status"]["id_str"]
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
        quote_of,
        retweet_count,
        favorite_count,
        possibly_sensitive,
        lang
        ) values ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        on conflict (twid) do nothing
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
            json.dumps(tweet["geo"]),
            json.dumps(tweet["coordinates"]),
            json.dumps(tweet["place"]),
            retweet_of,
            quote_of,
            tweet["retweet_count"],
            tweet["favorite_count"],
            tweet.get("possibly_sensitive"),
            tweet["lang"]
        ))
    except:
        from pprint import pprint
        pprint(tweet)
        raise

def loop():
    scheduled = True
    while scheduled:
        scheduled = False
        cur2 = conn.cursor()
        cur2.execute("select uid from metadata where tweets_requested is null limit 10000")
        print("")
        print("Scheduling downloads for batch of users...")
        for u in cur2:
            u = u[0]
            scheduled = True
            print(".", end="")
            sys.stdout.flush()
            cur.execute("update metadata set tweets_requested = current_timestamp where uid = %s", (u,))
            command("get", "statuses/user_timeline",
                    { "user_id": u, "trim_user": True, "count": 200, "include_rts": True, "exclude_replies": False },
                    "tweets",
                    { "user_id": u })
        conn.commit()

    print("")
    print("Processing tweets...")
    conn.commit()
    i = 0
    while True:
        i += 1
        res = get_or_wait_for_result("tweets")
        if not res:
            break
        print(".", end="")
        sys.stdout.flush()
        if isinstance(res["result"], list) and len(res["result"]) > 0 and "id_str" not in res["result"][0]:
            #cur.execute("update metadata set tweets_fetch_success = 'f' where uid = %s", (res["metadata"]["user_id"],))
            pass
        elif isinstance(res["result"], list):
            #cur.execute("update metadata set tweets_fetch_success = 't' where uid = %s", (res["metadata"]["user_id"],))
            for tweet in res["result"]:
                insert_tweet(tweet)
        else:
            with open("log", "w") as f:
                f.write("Oddity found for tweets response: "+json.dumps(res))
        finish_result(res)
        if i % 100 == 0:
            conn.commit()
    conn.commit()

loop()
