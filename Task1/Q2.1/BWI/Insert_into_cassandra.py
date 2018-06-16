#!/usr/bin/python

from cassandra.cluster import Cluster
import os

cluster = Cluster(['127.0.0.1'])
session = cluster.connect()
session.set_keyspace('cloudproject')

airport = 'BWI'
HDFS_Data = os.popen('/root/Hadoop/hadoop-2.7.3/bin/hadoop fs -cat hdfs://namenode:9000/Q2_1/BWI/part-00000')
#print HDFS_Data
HDFS_Data = HDFS_Data.readlines()
print HDFS_Data

for c in HDFS_Data :
	Carrier = c.split(':')[0]
	Dep_Delay = float(c.split(':')[1].strip())
	#print Dep_Delay
	session.execute("insert into airport_top_carrier (airport,carrier,Dep_Delay) values ('"+airport +"','"+Carrier+"',"+str(Dep_Delay)+")")	
	#print("insert into airport_top_carrier (airport,carrier,Dep_Delay) values ('"+airport +"','"+Carrier+"',"+str(Dep_Delay)+")")	
