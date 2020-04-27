import os 
import time
import subprocess
import signal

print("Running Integration Test: {}".format(__file__))

nodes = [
    ['1', ':8001', ':81'],
]

proc_list = []

path = os.path.join(os.getcwd(), 'boop_node')
proc = subprocess.Popen([
    'nohup', './boop_node', '1', ':8001', ':81'
])

time.sleep(20)

print("Completed Integration test")
proc.terminate()