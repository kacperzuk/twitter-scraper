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

def commands_in_queue(tag):
    cur.execute("select 1 from cmd_queue where tag = %s limit 1", (tag,))
    conn.commit()
    return bool(cur.fetchone())

def results_in_queue(tag):
    cur.execute("select 1 from res_queue where tag = %s limit 1", (tag,))
    conn.commit()
    return bool(cur.fetchone())

def loop():
    new_users = True
    while new_users:
        new_users = False
        cur.execute("select uid from metadata where user_requested is null for update skip locked limit 100")
        users = [ r[0] for r in cur.fetchall() ]
        if users:
            new_users = True
            for u in users:
                cur.execute("update metadata set user_requested = current_timestamp where uid = %s", (u,));
            command("post", "users/lookup",
                    { "user_id": ",".join(users)},
                    "users",
                    users)
        conn.commit()

    while True:
        print(".", end="")
        sys.stdout.flush()
        res = get_result_async("users")
        if not res:
            print("")
            if commands_in_queue("users") or results_in_queue("users"):
                res = get_result_sync("users")
            else:
                break
        successed_uids = []
        for u in res["result"]:
            successed_uids.append(u["id_str"])
            cur.execute("update metadata set user_fetch_success = 't' where uid = %s", (u["id_str"],))
            cur.execute("""
                insert into users (
                    uid,
                    name,
                    profile_image_url,
                    location,
                    created_at,
                    favourites_count,
                    utc_offset,
                    profile_use_background_image,
                    lang,
                    followers_count,
                    protected,
                    geo_enabled,
                    description,
                    verified,
                    notifications,
                    time_zone,
                    statuses_count,
                    friends_count,
                    screen_name
                ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                u["id_str"],
                u["name"],
                u["profile_image_url"],
                u["location"],
                u["created_at"],
                u["favourites_count"],
                u["utc_offset"],
                u["profile_use_background_image"],
                u["lang"],
                u["followers_count"],
                u["protected"],
                u["geo_enabled"],
                u["description"],
                u["verified"],
                u["notifications"],
                u["time_zone"],
                u["statuses_count"],
                u["friends_count"],
                u["screen_name"]
            ))
        for i in res["metadata"]:
            if i not in successed_uids:
                cur.execute("update metadata set user_fetch_success = 'f' where uid = %s", (u["id_str"],))
        conn.commit()
loop()
