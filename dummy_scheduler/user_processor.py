import time
import json
import sys

from common import conn, cur, get_response, ack_response, nack_response
from process_user import process_user

def handle_users_response(response):
    for u in response["result"]:
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
            ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) on conflict (uid) do nothing
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
    return True

while True:
    meta, resp = get_response("users")
    sys.stdout.flush()

    success = False
    try:
        if resp["tag"] == "users":
            print("u", end="")
            success = handle_users_response(resp)
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
