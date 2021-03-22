import requests
import json
import threading
from pprint import pprint
import random
import time
webserver_user = ""
webserver_passwd = ""
MAX_CONCURRENT_NUM = 200
SERVER_ADDRESS = "http://localhost:8080/api/v1/"
DAG_DIR = "dags/tutorial/dagRuns"


def make_a_request():
    random_ID = str(random.randint(7, 19991))
    base_json = {"dag_run_id": random_ID}
    result = requests.post(SERVER_ADDRESS+DAG_DIR
        ,
        data=json.dumps(base_json),
        auth=(webserver_user,webserver_passwd), headers={'Content-Type': 'application/json'})
    pprint(result.content.decode('utf-8'))

start_time = time.time()
threadpool = []
for i in range(MAX_CONCURRENT_NUM):
    th = threading.Thread(target=make_a_request, args=())
    threadpool.append(th)
for th in threadpool:
    th.start()
for th in threadpool:
    threading.Thread.join(th)
stop_time = time.time()
print("Total time cost:")
print(stop_time-start_time)