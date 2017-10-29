import sys
from common import conn, cur, command, aggregate, get_aggregate

def process_user(uid, follower_of=None, top_level_followee=None, nest_level=0, force_no_followers=False):
    cur.execute("insert into metadata (uid, follower_of, top_level_followee, nest_level) values (%s, %s, %s, %s) on conflict (uid) do update set nest_level = least(metadata.nest_level, excluded.nest_level)", (uid, follower_of, top_level_followee, nest_level))
    if follower_of and not force_no_followers:
        cur.execute("insert into followers (follower_uid, folowee_uid) values (%s, %s) on conflict (follower_uid, folowee_uid) do nothing", (uid, follower_of))
    if aggregate("users", uid) >= 100:
        users = get_aggregate("users", 100)
        command("post", "users/lookup", { "user_id": ",".join(users) }, "users")
    command("get", "followers/ids",
            { "user_id": uid, "stringify_ids": True },
            "followers",
            { "user_id": uid, "top_level_followee": top_level_followee, "nest_level": nest_level })
    command("get", "statuses/user_timeline",
            { "user_id": uid, "trim_user": True, "count": 200, "include_rts": True, "exclude_replies": False },
            "tweets",
            { "user_id": uid })

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Provide uid!")
        sys.exit(2)
    process_user(sys.argv[1])
    conn.commit()
