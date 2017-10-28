import time
import json
import sys
from common import conn, cur, get_response, ack_response, nack_response, command

def loop():
    new_users = True
    print("Scheduling batches of users")
    while new_users:
        new_users = False
        cur = conn.cursor()
        cur.execute("update metadata set user_requested = current_timestamp where uid in (select uid from metadata where user_requested is null limit 100 for update skip locked) returning uid")
        users = [ u[0] for u in cur ]
        conn.commit()
        if users:
            print(".", end="")
            sys.stdout.flush()
            new_users = True
            command("post", "users/lookup",
                    { "user_id": ",".join(users)},
                    "users",
                    users)
            conn.commit()

    print("")
    print("Processing responses")
    while True:
        meta, res = get_response("users")
        if not res:
            break
        print(".", end="")
        sys.stdout.flush()

        successed_uids = []
        for u in res["result"]:
            if u == "error":
                continue
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
            conn.commit()
        for i in res["metadata"]:
            if i not in successed_uids:
                cur.execute("update metadata set user_fetch_success = 'f' where uid = %s", (i,))
                conn.commit()
        ack_response(meta)
        conn.commit()
loop()
