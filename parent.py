#!/usr/bin/env python3

import subprocess
import time
import os
import sys
import socket


(r1, w1) = os.pipe2(0)
(r2, w2) = os.pipe2(0)
outfile = os.fdopen(w1, 'w')
infile = os.fdopen(r2, 'r')

    
# Start zookeeper
zookeeper = subprocess.Popen(['./zookeeper-2.py', str(r1), str(w2)], pass_fds=(r1, w2))
time.sleep(1)

# Start kafka brokers
kafka_brokers = []


for i in range(3):
    # create pipes
    
    child = subprocess.Popen(['./broker-3.py', str(i)])
    print("Child: ", i)
    
    kafka_brokers.append(child)
    
    '''
    print('Foo', file=outfile)
    print(infile.readline(), end='')
    print('bar', file=outfile)
    print(infile.readline(), end='')
    print('quit', file=outfile)
    '''

    # create sockets for communication
    '''
    s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s1.bind(('localhost', 3567+i))
    s1.listen(10)
    '''
    #conn, addr = s1.accept()
    #kafka_brokers.append(subprocess.Popen(['./broker.py', str(i)], stdin = subprocess.PIPE, stdout = sys.stdout))
    #kafka_brokers.append(subprocess.Popen(['./broker.py'], stdin = s1, stdout = s1))
    
for i in kafka_brokers:
	i.wait()

zookeeper.wait()
