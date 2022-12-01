#!/usr/bin/env python3

# Working behind zookeper

import sys
import os
import socket
from _thread import *
from time import sleep
import pickle
import traceback
import json
import time

list_of_brokers = {}
list_of_producers = []
list_of_consumers = []
topics = {}
topic_offset=dict()

if not os.path.exists("meta.json"):
	with open("meta.json", "w") as f:
		topic_offset=dict()
		json.dump(topic_offset,f,indent=4)

s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s1.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
try:
	
	def brokerthread(conn, addr):
		
		while True:
			msg = conn.recvmsg(2048)[0]
			
			node, topic, partition, msg, offset = msg.decode().split("\n")
			
				
			with open("meta.json",'r') as file:
				metalog=json.load(file)
				metalog[topic]=offset
			with open("meta.json",'w') as file:
				json.dump(metalog,file,indent=4)
		
	def hbthread(conn, addr):
		while True:
			timer = time.time()
			res = timer + 7
			while time.time() < res:
				msg = conn.recvmsg(2048)[0]
			try:
				if len(msg.decode()) > 0:
					#print("Everything is great!")
					pass
			except:
				print("Broker error")
					
	
	port = 3800
	s1.bind(('localhost', 3800))
	s1.listen(100)
	print("Zookeeper listening")

	i = 0
	while i<6:
		conn, addr = s1.accept()
		print (addr[0] + " connected at " + str(addr[1]))
		msg = conn.recv(2048).decode()
		if msg == "heartbeat":
			
			start_new_thread(hbthread, (conn, addr))
			i+=1
		else:
			broker_port = int(msg)
			print("Broker port: ", broker_port)
			list_of_brokers[broker_port] = conn
			i+=1
			start_new_thread(brokerthread, (conn, addr))
		
	
	try:
		with open('metadata.pkl', 'rb') as f:
			topics = pickle.load(f)
	except:
		topics = {}
		
	def get_broker(partition, topic):
		for x in topics[topic]:
			if(x[0] == partition):
				return x[1]

	p = 0
	rep_factor = 2
	def clientthread(conn, addr,list_of_clients, topic):
		
		
		# start publishing data
		# brokers need to create log files for the partitions
		#conn.send(topics[topic].encode())
		print(topics[topic])
		for i in topics[topic]:
			conn.sendmsg([str(i[0]).encode(), "\n".encode(), str(i[1]).encode(), "\n".encode()])
			sleep(1)
			#conn.send(str(i[1]).encode())
		
	
	def consumerthread(conn, addr,list_of_clients, topic):
		
		
		# start publishing data
		# brokers need to create log files for the partitions
		#conn.send(topics[topic].encode())
		print(topics[topic])
		for i in topics[topic]:
			conn.sendmsg([str(i[0]).encode(), "\n".encode(), str(i[1]).encode(), "\n".encode()])
			sleep(1)
			#conn.send(str(i[1]).encode())
			
		
	def stdinthread():
		global p
		for line in sys.stdin:
			message = line.strip()
			print("zookeeper: ", port, message)
			if message == "":
				break
			else:
				# If the message is for the creation of a topic
				if message.startswith("create"):
					# Get the topic name
					topic = message.split(" ")[1]
					topics[topic] = []
					with open("meta.json",'r') as file:
						metalog=json.load(file)
						metalog[topic]="-1"
					with open("meta.json",'w') as file:
						json.dump(metalog,file,indent=4)
					partitions = message.split(" ")[2]
					for k in range(int(partitions)):
						topics[topic].append((k, list(list_of_brokers.keys())[p]))
						p = (p+1) % len(list_of_brokers)
						
					with open('metadata.pkl', 'wb') as f:
						pickle.dump(topics, f)
					for brokers in list_of_brokers.values():

						brokers.send(message.encode())
					

	start_new_thread(stdinthread,())

	while True:	
		
		
		conn, addr = s1.accept()
		print (addr[0] + " connected")
		try:
			
			msg = conn.recvmsg(2048)[0]
			msg, topic = msg.decode().split("\n")
			print(msg, topic)
			print("Message: ", msg)
			if(msg == "producer"):
				list_of_producers.append(conn)
				conn.send(str(len(topics[topic])).encode())
				sleep(1)
				start_new_thread(clientthread,(conn,addr,list_of_producers, topic))
				
			elif (msg == "consumer"):
				
				list_of_consumers.append(conn)
				print("Topics: ", topics[topic])
				conn.send(str(len(topics[topic])).encode())
				sleep(1)
				for i in topics[topic]:
					conn.sendmsg([str(i[0]).encode(), "\n".encode(), str(i[1]).encode(), "\n".encode()])
					sleep(1)
				with open("meta.json","r") as f:
					metalog=json.load(f)
					print(metalog)
					conn.send(metalog[topic].encode())
			
		except Exception as e:
			print(traceback.format_exc())
			print(e, "Error in message receving")
	
except Exception as e:
	print(e)
	print(traceback.format_exc())
	with open('metadata.pkl', 'wb') as f:
		pickle.dump(topics, f)
	s1.close()
    
