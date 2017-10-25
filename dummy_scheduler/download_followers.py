import time
import json
import sys

from common import conn, cur, get_or_wait_for_result, command, finish_result

if len(sys.argv) < 3:
    print("Provide max_nest_level and screen_name to scan for.")
    sys.exit(1)

def screen_name_to_uid(screen_name):
    tag = "id_of_"+screen_name
    command("get", "users/lookup", { "screen_name": screen_name }, tag)
    conn.commit()
    res = get_or_wait_for_result(tag)
    finish_result(res)
    return res["result"][0]["id"]

def loop(max_nest_level):
    new_users = True
    while new_users:
        new_users = False
        cur2 = conn.cursor()
        cur2.execute("select uid, nest_level from metadata where followers_requested is null and nest_level < %s limit 10000", (max_nest_level,))
        print("")
        print("Scheduling followers request for batch of users")
        for row in cur2:
            new_users = True
            print(".", end="")
            cur.execute("update metadata set followers_requested = current_timestamp where uid = %s", (row[0],));
            command("get", "followers/ids",
                    { "user_id": row[0], "stringify_ids": True},
                    "followers",
                    { "user_id": row[0], "nest_level": row[1] })
        conn.commit()

    print("")
    print("Processing responses...")
    i = 0
    while True:
        res = get_or_wait_for_result("followers")
        i += 1
        if not res:
            break
        print(".", end="")

        if "next_cursor" in res["result"] and res["result"]["next_cursor"] != 0:
            command("get", "followers/ids",
                    { "user_id": res["metadata"]["user_id"], "stringify_ids": True,
                      "cursor": res["result"]["next_cursor"] },
                    "followers",
                    res["metadata"])

        if "ids" in res["result"]:
            cur.execute("update metadata set followers_fetch_success = 't' where uid = %s", (res["metadata"]["user_id"],))
            for follower in res["result"]["ids"]:
                new_users = True
                nest_level = res["metadata"]["nest_level"] + 1
                cur.execute("insert into metadata (uid, nest_level) values (%s, %s) on conflict (uid) do update set nest_level = least(metadata.nest_level, excluded.nest_level)", (follower, nest_level))
                cur.execute("insert into followers (follower_uid, folowee_uid) values (%s, %s) on conflict (follower_uid, folowee_uid) do nothing", (follower, res["metadata"]["user_id"]))
        else:
            cur.execute("update metadata set followers_fetch_success = 't' where uid = %s", (res["metadata"]["user_id"],))
        finish_result(res)
        if i % 1000 == 0:
            conn.commit()


if len(sys.argv) > 2:
    cur.execute("insert into metadata (uid) values (%s) on conflict (uid) do update set nest_level = 0", (screen_name_to_uid(sys.argv[2]),))
    conn.commit()

loop(sys.argv[1])
