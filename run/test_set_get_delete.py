import os 
import time
import subprocess
import signal
import requests
import secrets

print('='*81)
print("Running Integration Test: {}".format(__file__))

nodes = [
    ['1', '0.0.0.0:8001', '0.0.0.0:81'],
    ['2', '0.0.0.0:8002', '0.0.0.0:82'],
    ['3', '0.0.0.0:8003', '0.0.0.0:83'],
    ['4', '0.0.0.0:8004', '0.0.0.0:84'],
    ['5', '0.0.0.0:8005', '0.0.0.0:85'],
]

proc_list = []

print('-'*81)
print("Initializing nodes")
for node in nodes:
    proc = subprocess.Popen([
        'nohup', './boop_node', node[0], node[1], node[2]
    ])
    proc_list.append(proc)

time.sleep(1)
print("Completed Initialization")
print('-'*81)

# Run Routine
# Join
for node in nodes:
    id = int(node[0]) % len(nodes)
    res = requests.post('http://' + node[2] + '/join',
        json={
            'id': nodes[id][0], 
            'address': nodes[id][1]
        }
    )

# Set value
id = 1
test_dict = {}
for node in nodes:
    v = secrets.token_urlsafe(5)
    test_dict[id] = v
    res = requests.post('http://' + node[2] + '/set',
        json={
            "key": str(id),
            "value": v 
        }
    )

    data = res.json()
    print(data)
    if not data['error']:
        print("Test Passed For Node {}".format(node[0]))
    else:
        print("Test Failed For Node {}".format(node[0]))

    id += 1

print('-'*81)

id = 1
for node in nodes:
    res = requests.post('http://' + node[2] + '/get',
        json={
            "key": str(id),
        }
    )
    data = res.json()
    
    print(data)
    if data['value'] == test_dict[id]:
        print("Test Passed For Node {}".format(node[0]))
    else:
        print("Test Failed For Node {}".format(node[0]))

    id += 1

print('-'*81)

id = 1
for node in nodes:
    res = requests.post('http://' + node[2] + '/delete',
        json={
            "key": str(id),
        }
    )
    data = res.json()
    
    print(data)
    if not data['error']:
        print("Test Passed For Node {}".format(node[0]))
    else:
        print("Test Failed For Node {}".format(node[0]))

    id += 1

print('-'*81)

print("Completed Integration test")
for proc in proc_list:
    print("Kill PID: {}".format(proc.pid))
    proc.terminate()

print('='*81)