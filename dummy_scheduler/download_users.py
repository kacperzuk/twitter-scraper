import time
import json
import sys
from common import conn, cur, get_or_wait_for_result, finish_result, command

def loop():
    new_users = True
    while new_users:
        new_users = False
        print("Scheduling batches of users")
        cur2 = conn.cursor()
        cur2.execute("select uid from metadata where user_requested is null limit 10000")
        for u in cur2:
            print(".", end="")
            u = u[0]
            new_users = True
            cur.execute("update metadata set user_requested = current_timestamp where uid = %s", (u,));
            command("post", "users/lookup",
                    { "user_id": ",".join(users)},
                    "users",
                    users)
        conn.commit()

    print("")
    print("Processing responses")
    i = 0
    while True:
        i += 1
        res = get_or_wait_for_result("users")
        if not res:
            break
        print(".", end="")
        sys.stdout.flush()

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
        finish_result(res)
        if i % 1000 == 0:
            conn.commit()
loop()
