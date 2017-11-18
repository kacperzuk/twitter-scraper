import sys
import subprocess
import json
import psycopg2.extras
from common import conn, command, aggregate, get_aggregate

def handler(obj):
    if hasattr(obj, 'isoformat'):
        return obj.isoformat()
    else:
        raise TypeError('Object of type %s with value of %s is not JSON serializable' % (type(obj), repr(obj)))

cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

def prepare(table, check_column):
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name=%s and column_name=%s", (table, check_column))
    if not cur.fetchone():
        cur.execute("alter table %s add column %s boolean" % (table, check_column))
    conn.commit()

def run(table, check_column, update_column, id_column, app):
    p = subprocess.Popen([ app ], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    while True:
        cur.execute("select * from %s where %s is distinct from 't' for update skip locked limit 1" % (table, check_column))
        r = cur.fetchone()
        if not r:
            break
        print("Processing row %s..." % r[id_column], end="")
        sys.stdout.flush()
        p.stdin.write((json.dumps(r, default=handler)+"\n").encode())
        p.stdin.flush()
        res = p.stdout.readline()
        try:
            res = json.loads(res)
        except:
            print(" Fail.")
            print(res)
            raise
        if not update_column in res or not id_column in res:
            print(" Fail.")
            print(res)
            raise Exception("Missing %s key, broken app!" % (update_column, id_column))
        cur.execute("update %s set %s = 't', %s = %s where %s = %s" % (table, check_column, update_column, '%s', id_column, '%s'), (res[update_column], res[id_column]))
        print(" Done.")
        conn.commit()

if __name__ == "__main__":
    if len(sys.argv) < 6:
        print("Provide table, check_column, update_column, id_column and app!")
        sys.exit(2)
    print("Prepare start")
    prepare(sys.argv[1], sys.argv[2])
    print("RUN")
    run(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
