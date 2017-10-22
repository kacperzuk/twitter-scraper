import psycopg2
import time
import json
import os
import sys

if len(sys.argv) < 3:
    print("Provide max_nest_level and screen_name to scan for.")
    sys.exit(1)

conn = psycopg2.connect("dbname=%s user=%s host=%s password=%s" % (
    os.getenv("PGDATABASE"),
    os.getenv("PGUSER"),
    os.getenv("PGHOST"),
    os.getenv("PGPASSWORD")))

cur = conn.cursor()
cur.execute("create table if not exists users ( uid varchar(100) primary key, followers_fetched timestamp, tweets_fetched timestamp, nest_level integer default 0)")
cur.execute("create table if not exists followers ( follower_uid varchar(100) references users(uid), folowee_uid varchar(100) references users(uid), primary key(follower_uid, folowee_uid))")
conn.commit()

def screen_name_to_uid(screen_name):
    tag = "id_of_"+screen_name
    command("get", "users/lookup", { "screen_name": screen_name }, tag)
    res = get_result_sync(tag)["result"]
    return res[0]["id"]

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

def loop(max_nest_level):
    new_users = True
    while new_users:
        new_users = False
        cur2 = conn.cursor()
        cur2.execute("select uid, nest_level from users where followers_fetched is null and nest_level < %s", (max_nest_level,))
        for row in cur2:
            cur.execute("update users set followers_fetched = current_timestamp where uid = %s", (row[0],));
            command("get", "followers/ids",
                    { "user_id": row[0], "stringify_ids": True},
                    "followers",
                    { "user_id": row[0], "nest_level": row[1] })
        conn.commit()

        while commands_in_queue("followers") > 0 or results_in_queue("followers") > 0:
            res = get_result_sync("followers")
            if res["result"]["next_cursor"] != 0:
                command("get", "followers/ids",
                        { "user_id": res["metadata"]["user_id"], "stringify_ids": True,
                          "cursor": res["result"]["next_cursor"] },
                        "followers",
                        res["metadata"])
            for follower in res["result"]["ids"]:
                new_users = True
                nest_level = res["metadata"]["nest_level"] + 1
                cur.execute("insert into users (uid, nest_level) values (%s, %s) on conflict (uid) do update set nest_level = least(users.nest_level, excluded.nest_level)", (follower, nest_level))
                cur.execute("insert into followers (follower_uid, folowee_uid) values (%s, %s) on conflict (follower_uid, folowee_uid) do nothing", (follower, res["metadata"]["user_id"]))
            conn.commit()


    print("Done.")
    cur.execute("select count(*) from users")
    conn.commit()
    print("Total users in DB: %d" % (cur.fetchone()[0],))


if len(sys.argv) > 2:
    cur.execute("insert into users (uid) values (%s) on conflict (uid) do update set nest_level = 0", (screen_name_to_uid(sys.argv[2]),))
    conn.commit()

loop(sys.argv[1])
