#!/usr/bin/env python3

import sys
import os
import socket
from _thread import *
import argparse

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.connect(("localhost", 3750))

#topic = sys.argv[1]
list_of_partitions = []

parser = argparse.ArgumentParser()
parser.add_argument('topic', type=str)
parser.add_argument('--from_beginning', action='store_true')
args = parser.parse_args()

topic = args.topic

offset = 0

server.sendmsg(["consumer".encode(), "\n".encode(), topic.encode()])

for i in range(3):
	msg = server.recvmsg(2048)
	print(msg)
	meta = msg[0].decode().split('\n')
	print(meta)
	#for i in range(0, len(msg), 2):
		
	meta1 = meta[0]
	meta2 = meta[1]
	print("consumer: ", meta1, meta2)
	#meta2 = server.recv(2048).decode()
	list_of_partitions.append((int(meta1), int(meta2)))
	
cons_broks = {}
print(list_of_partitions)
		

cons_brok = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
cons_brok.connect(('localhost', list_of_partitions[0][1]))

offset = int(server.recv(2048).decode()) + 1
print(offset)

for i in range(len(list_of_partitions)):

	def consumerthread(path, f, l):
		global offset
		#print(path)
		#print("\n")
		while True:
			if os.path.getsize(path) != 0:
				
				line = f.readline().decode().strip().split(":")
				len_line = len(":".join(line))
				if(len(line)>1):
					if(int(line[0]) == offset):
						print(line[1])
						offset = offset + 1
					else:
						if args.from_beginning:
							f.seek(-1*(len_line+1), os.SEEK_CUR)
						else:
							f.seek(0, os.SEEK_CUR)
						
			
	path = "broker"+str(list_of_partitions[0][1])+"/"+topic+"/"+str(i)+".log"
	print(path)
	f = open(path, "rb")
	#f.seek(-1,2)
	flag = 0
	l = []
	if args.from_beginning:
		if os.path.getsize(path) != 0:
			f.seek(0)
			offset = 0
	else:		
		if os.path.getsize(path) != 0:
			l = f.readlines()
			f.seek(0)
			if len(l) == 1:
				f.seek(0)
			else:
				f.seek(-2, os.SEEK_END)
				while f.read(1) != b'\n':
			    		f.seek(-2, os.SEEK_CUR)
		else:
			f.seek(0)
			flag = 1
	
		
	start_new_thread(consumerthread,(path, f, l))
	
input("Press enter to exit\n")


