import os 
import time
import subprocess
import signal
import requests

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
for node in nodes:
    res = requests.get('http://' + node[2] + '/ping')

    data = res.json()
    print(data)
    if not data['error']:
        print("Test Passed For Node {}".format(node[0]))

print('-'*81)

print("Completed Integration test")
for proc in proc_list:
    print("Kill PID: {}".format(proc.pid))
    proc.terminate()

print('='*81)