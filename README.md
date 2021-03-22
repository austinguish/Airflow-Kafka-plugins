# Airflow Benchmark

## 1. Test Environment

 Hostmachine:

| System        | Linux Kernel Version | CPU               | Ram  | Airflow version |
| ------------- | -------------------- | ----------------- | ---- | --------------- |
| Kubuntu 20.04 | 5.4.0-67-generic     | Intel 7700HQ 4c8t | 16GB | 2.01            |

 Network：

| Network            | Bandwidth | Router       |
| ------------------ | --------- | ------------ |
| CERNNET East China | 1000Mbps  | Redmi AC2100 |

## 2. Concurrent test

**Condition**: using `python ` script set a 10000 thread pool to send `POST` request to the airflow server.

```python
import requests
import json
import threading
from pprint import pprint
import random
webserver_user = ""
webserver_passwd = ""
MAX_CONCURRENT_NUM = 500
SERVER_ADDRESS = "http://localhost:8080/api/v1/"
DAG_DIR = "dags/example_external_task_marker_child/dagRuns"


def make_a_request():
    random_ID = str(random.randint(7, 19991))
    base_json = {"dag_run_id": random_ID}
    result = requests.post(SERVER_ADDRESS+DAG_DIR
        ,
        data=json.dumps(base_json),
        auth=(webserver_user,webserver_passwd), headers={'Content-Type': 'application/json'})
    pprint(result.content.decode('utf-8'))


threadpool = []
for i in range(MAX_CONCURRENT_NUM):
    th = threading.Thread(target=make_a_request, args=())
    threadpool.append(th)
for th in threadpool:
    th.start()
for th in threadpool:
    threading.Thread.join(th)

```

**The Airflow Scheduler output** 

 `DAG example_external_task_marker_child already has 29 active runs, not queuing any tasks for run 2021-03-22 08:08:27.212848+00:00`

It means that the max size of the task queue is 29. Maybe could be modified to support more requests.

**The Max_request_number**

The `maximum_page_limit` in the [official doc](https://airflow.apache.org/docs/apache-airflow/stable/security/api.html#page-size-limit) is 100. In the reality test, when the `MAX_CUCURRENCY_NUMBER` up to 2000, I got a request error.



