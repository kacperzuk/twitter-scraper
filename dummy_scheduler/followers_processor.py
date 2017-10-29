import time
import json
import sys

from common import conn, cur, get_response, command, ack_response, nack_response
from process_user import process_user

def handle_followers_response(res):
    if "next_cursor" in res["result"] and res["result"]["next_cursor"] != 0:
        command("get", "followers/ids",
                { "user_id": res["metadata"]["user_id"], "stringify_ids": True,
                  "cursor": res["result"]["next_cursor"] },
                "followers",
                res["metadata"])

    if "ids" not in res["result"]:
        return True

    for follower in res["result"]["ids"]:
        nest_level = res["metadata"]["nest_level"] + 1
        process_user(follower, res["metadata"]["user_id"], res["metadata"]["top_level_followee"], nest_level, nest_level <= int(sys.argv[1]))
    conn.commit()
    return True

if len(sys.argv) < 2:
    print("Provide max nest_size!")
    sys.exit(1)

while True:
    meta, resp = get_response("followers")
    sys.stdout.flush()

    success = False
    try:
        if resp["tag"] == "followers":
            print("f", end="")
            success = handle_followers_response(resp)
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
