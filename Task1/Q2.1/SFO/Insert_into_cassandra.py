#!/usr/bin/python

from cassandra.cluster import Cluster
import os

cluster = Cluster(['127.0.0.1'])
session = cluster.connect()
session.set_keyspace('cloudproject')

airport = 'SFO'
HDFS_Data = os.popen('/root/Hadoop/hadoop-2.7.3/bin/hadoop fs -cat hdfs://namenode:9000/Q2_1/SFO/part-00000')
HDFS_Data = HDFS_Data.readlines()

for c in HDFS_Data :
	Carrier = c.split(':')[0]
	Dep_Delay = float(c.split(':')[1].strip())
	session.execute("insert into airport_top_carrier (airport,carrier,Dep_Delay) values ('"+airport +"','"+Carrier+"',"+str(Dep_Delay)+")")	
