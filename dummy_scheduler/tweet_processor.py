import time
import json
import sys

from common import conn, cur, get_response, command, ack_response, nack_response

def insert_tweet(tweet):
    retweet_of = None
    if "retweeted_status" in tweet and tweet["retweeted_status"]:
        insert_tweet(tweet["retweeted_status"])
        retweet_of = tweet["retweeted_status"]["id_str"]
    quote_of = None
    if "quoted_status" in tweet and tweet["quoted_status"]:
        insert_tweet(tweet["quoted_status"])
        quote_of = tweet["quoted_status"]["id_str"]
    uid = tweet["user"]["id_str"]
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

def handle_tweets_response(resp):
    if isinstance(resp["result"], list):
        for tweet in resp["result"]:
            insert_tweet(tweet)
    elif "code" in resp["result"] and resp["result"]["code"] == 34: # page does not exist
        pass
    elif not resp["result"]:
        pass
    else:
        raise Exception("Unknown format.")
    conn.commit()
    return True

while True:
    meta, resp = get_response("tweets")
    sys.stdout.flush()

    success = False
    try:
        if resp["tag"] == "tweets":
            print("t", end="")
            sys.stdout.flush()
            success = handle_tweets_response(resp)
        else:
            raise Exception("Unexpected tag")
    except:
        nack_response(meta)
        print("\nException occured when handling this response:")
        print(resp)
        raise

    if success:
        ack_response(meta)
    else:
        nack_response(meta)
