import psycopg2
import os

conn = psycopg2.connect("dbname=%s user=%s host=%s password=%s" % (
    os.getenv("PGDATABASE"),
    os.getenv("PGUSER"),
    os.getenv("PGHOST"),
    os.getenv("PGPASSWORD")))

cur = conn.cursor()
try:
    cur.execute("create type cmd_method as enum ('get', 'post');")
except psycopg2.ProgrammingError:
    conn.rollback()
cur.execute("""
create table if not exists metadata (
    uid varchar(100) primary key,
    followers_requested timestamp,
    followers_fetch_success boolean,
    tweets_requested timestamp,
    tweets_fetch_success boolean,
    user_requested timestamp,
    user_fetch_success boolean,
    nest_level integer default 0
)""")
cur.execute("""
create table if not exists followers (
    follower_uid varchar(100),
    folowee_uid varchar(100),
    primary key(follower_uid, folowee_uid)
)""")
cur.execute("""
create table if not exists users (
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
create table if not exists tweets (
    twid varchar(100) primary key,
    uid varchar(100) not null,
    tweet text not null,
    created_at timestamp not null
)""")
cur.execute("""
create table if not exists cmd_queue (
    id serial primary key,
    method cmd_method not null,
    path text not null,
    params json,
    tag text not null,
    metadata json,
    is_processing_since timestamp,
    tries int not null default '0'
)""")
cur.execute("""
create table if not exists res_queue (
    id serial primary key,
    tag text not null,
    metadata json,
    result json
)""")

cur.execute("truncate users");
cur.execute("truncate metadata");
cur.execute("truncate tweets");
cur.execute("truncate followers");
cur.execute("truncate cmd_queue");
cur.execute("truncate res_queue");
conn.commit()
