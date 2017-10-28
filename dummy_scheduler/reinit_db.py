import psycopg2
import os

conn = psycopg2.connect("dbname=%s user=%s host=%s password=%s" % (
    os.getenv("PGDATABASE"),
    os.getenv("PGUSER"),
    os.getenv("PGHOST"),
    os.getenv("PGPASSWORD")))

cur = conn.cursor()
cur.execute("drop table if exists users")
cur.execute("drop table if exists metadata")
cur.execute("drop table if exists tweets")
cur.execute("drop table if exists followers")
cur.execute("""
create table metadata (
    uid varchar(100) primary key,
    followers_requested timestamp,
    followers_fetch_success boolean,
    tweets_requested timestamp,
    tweets_fetch_success boolean,
    user_requested timestamp,
    user_fetch_success boolean,
    nest_level integer not null default 0
)""")
cur.execute("""
create table followers (
    follower_uid varchar(100) not null,
    folowee_uid varchar(100) not null,
    primary key(follower_uid, folowee_uid)
)""")
cur.execute("""
create table users (
    uid varchar(100) primary key,
    name text,
    profile_image_url text,
    location text,
    created_at text,
    favourites_count integer,
    utc_offset integer,
    profile_use_background_image boolean,
    lang text,
    followers_count integer,
    protected boolean,
    geo_enabled boolean,
    description text,
    verified boolean,
    notifications boolean,
    time_zone text,
    statuses_count integer,
    friends_count integer,
    screen_name text
)""")
cur.execute("""
create table tweets (
    twid varchar(100) primary key,
    uid varchar(100) not null,
    tweet text not null,
    created_at timestamp not null,
    truncated boolean,
    hashtags text,
    symbols text,
    user_mentions text,
    urls text,
    in_reply_to_status_id varchar(100),
    in_reply_to_user_id varchar(100),
    in_reply_to_screen_name text,
    geo text,
    coordinates text,
    place text,
    retweet_of varchar(100),
    quote_of varchar(100),
    retweet_count integer,
    favorite_count integer,
    possibly_sensitive boolean,
    lang text
)""")
conn.commit()
