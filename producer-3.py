#!/usr/bin/env python3

import sys
import os
import socket
from _thread import *
from time import sleep
import traceback

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.connect(("localhost", 3750))

topic = sys.argv[1]
offset = 0

def get_broker(partition, meta):
	for x in meta:
		if(x[0] == partition):
			return x[1]
			
			

server.sendmsg(["producer".encode(), "\n".encode(), topic.encode()])
# Get the mapping of partitions and their leaders
#msg = server.recv(2048)
list_of_partitions = []

for i in range(3):
	msg = server.recvmsg(2048)
	print(msg)
	meta = msg[0].decode().split('\n')
	print(meta)
	#for i in range(0, len(msg), 2):
		
	meta1 = meta[0]
	meta2 = meta[1]
	print("producer: ", meta1, meta2)
	#meta2 = server.recv(2048).decode()
	list_of_partitions.append((int(meta1), int(meta2)))
	#list_of_partitions = msg.decode() # [(0, 4567), (1, 4568)]

prod_broks = {}
print(list_of_partitions)

for i in range(len(list_of_partitions)):
	prod_brok = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	prod_brok.connect(('localhost', list_of_partitions[i][1]))
	prod_broks[list_of_partitions[i][1]] = prod_brok


while True:
	message = sys.stdin.readline()
	print("Message: ", message)
	letter = message.strip()[0]
	partition = ord(letter) % len(list_of_partitions)
	port = get_broker(partition, list_of_partitions)
	print(message, port)
	prod_broks[port].sendmsg(["producer".encode(), "\n".encode(), topic.encode(), "\n".encode(), repr(partition).encode(), "\n".encode(), message.encode(), repr(offset).encode()])
	ack = prod_broks[port].recv(2048)
	if(ack.decode() == "ACK"):
		print("ACK")
		offset += 1
	else:
		print("Send the message again.")

	
