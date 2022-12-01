#!/usr/bin/env python3

import os
import sys
import socket
import time
from time import sleep
from _thread import *
import traceback
broker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
broker.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

try:
	port = 5005+ int(sys.argv[1])

	def clientthread():
		while True:
			message = server.recv(2048)
			if not message:
				break
			else:

				if message.decode().startswith("create"):
					# Get the topic name
					print(message)
					topic = message.decode().split(" ")[1]
					
					# Each topic should be created a directory in the file system
					if not os.path.exists("broker"+str(port)+"/"):
						os.mkdir("broker"+str(port)+"/")
					os.mkdir("broker"+str(port)+"/"+topic)
					# Create a log file for each broker
					log_file = open("log" + str(port) +".txt", "a")

					partitions = message.decode().split(" ")[2]
					for partition in range(int(partitions)):
						path = "broker"+str(port)+"/"+topic+"/"+str(partition)+".log"
						f = open(path, "a")
						f.close()

						if len(broker_conns) == 0:
							for i in broker_ports:
								if i != port:
									
									conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
									conn.connect(('localhost', i))

									broker_conns.append(conn)
						#for i in broker_conns:
							#i.sendmsg(["broker-create".encode(), "\n".encode(), topic.encode(), str(partition).encode()])		
	
	def hbthread():
		while True:
			server_hb.sendmsg([str(port).encode()])
			sleep(7)						
				    
	# Zookeeper-broker communication
	server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	server.connect(('localhost', 3800))
	server.send(str(port).encode())
	sleep(1)
	
	server_hb = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	server_hb.connect(('localhost', 3800))
	server_hb.send("heartbeat".encode())
	sleep(1)
	start_new_thread(hbthread, ())
	
	print("Port: ", port)
	broker.bind(('localhost', port))
	broker.listen(100)

	broker_ports = [5005,5006,5007]
	broker_conns = []

	start_new_thread(clientthread,())


	#server.sendmsg(("Connected").encode())


	def datathread(conn, addr):
		# get message data
		while True:
			message = conn.recvmsg(2048)[0]
			
			if message:
				if message.decode() == "ACK":
					continue
				
				node, topic, partition, msg, offset = message.decode().split("\n")
				
				if node == "producer":
					
					#node, topic, partition, msg, offset = message.decode()
					path = "broker"+str(port)+"/"+topic+"/"+partition+".log"
					print(path)
					f = open(path, "a")
					if not os.path.exists(path):
						f = open(path, "a")
						f.write(":".join([offset, msg]))
					else:
						x = f.write(":".join([offset, msg]))
						f.write("\n")
						f.close()
					server.sendmsg(["broker".encode(), "\n".encode(),topic.encode(), "\n".encode(),partition.encode(), "\n".encode(),msg.encode(), "\n".encode(),offset.encode()])
					
					if len(broker_conns) == 0:
						for i in broker_ports:
							if i != port:
								#conn, addr = broker.accept()
								print ("Brokers: ", addr[0] + " connected")
								conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
								conn.connect(('localhost', i))
			
								broker_conns.append(conn)
					for i in broker_conns:
						i.sendmsg(["broker".encode(), "\n".encode(), topic.encode(), "\n".encode(), partition.encode(), "\n".encode(), msg.encode(), "\n".encode(), offset.encode()])		
					conn.send("ACK".encode())
					
				elif node == "broker":
					#node, topic, partition, msg, offset = message.decode()
					path = "broker"+str(port)+"/"+topic+"/"+partition+".log"
					if not os.path.exists(path):
						f = open(path, "a")
						f.write(":".join([offset, msg]))
					else:
						f = open(path, "a")
						f.write(":".join([offset, msg]))
						f.write("\n")
						f.close()
					conn.send("ACK".encode())
				
	while True:
		
		try:
			conn, addr = broker.accept()
			print (addr[0] + " connected at " + str(addr[1]))
			start_new_thread(datathread,(conn, addr))
		except:
			continue
		
except Exception as e:
	print(e)
	print(traceback.format_exc())
	broker.close()			
	



