import sys
import subprocess
import json
from common import conn, cur, command, aggregate, get_aggregate

def prepare(table, column_name, column_type):
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name=%s and column_name=%s", (table, column_name))
    if not cur.fetchone():
        cur.execute("alter table %s add column %s %s" % (table, column_name, column_type))
    conn.commit()

def run(table, column, id_column, app):
    p = subprocess.Popen([ app ], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    while True
        cur.execute("select * from %s for update where %s is null skip locked limit 1" % (table, column))
        r = cur.fetchone()
        if not r:
            break
        p.stdin.write(json.dumps(r)+"\n")
        res = p.stdout.readline()
        if not column in res:
            print(res)
            raise Exception("Missing %s key, broken app!" % column)
        cur.execute("update %s set %s = %s where %s = %s" % (table, column, '%s', id_column, '%s'), (res[column], res[id_column]))
        conn.commit()

if __name__ == "__main__":
    if len(sys.argv) < 5:
        print("Provide table, column_name, column_type, id_column and app!")
        sys.exit(2)
    prepare(sys.argv[1], sys.argv[2], sys.argv[3])
    run(sys.argv[1], sys.argv[2], sys.argv[4], sys.argv[5])
