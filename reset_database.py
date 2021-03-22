import sys
import sqlite3

conn = sqlite3.connect('./airflow.db')
c = conn.cursor()

dag_input = sys.argv[1]

for t in [ "task_instance", "sla_miss", "log", "job", "dag_run"]:
    query = "delete from {} where dag_id='{}'".format(t, dag_input)
    c.execute(query)

conn.commit()
conn.close()
