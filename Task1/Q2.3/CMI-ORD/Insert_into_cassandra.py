#!/usr/bin/python

from cassandra.cluster import Cluster
import os

cluster = Cluster(['127.0.0.1'])
session = cluster.connect()
session.set_keyspace('cloudproject')

Origin = 'CMI'
Dest = 'ORD'
HDFS_Data = os.popen('/root/Hadoop/hadoop-2.7.3/bin/hadoop fs -cat hdfs://namenode:9000/Q2_3/CMI-ORD/part-00000')
HDFS_Data = HDFS_Data.readlines()

for c in HDFS_Data :
	carrier = c.split(':')[0]
	Avg_Arr_Delay = float(c.split(':')[1].strip())
	session.execute("insert into origin_dest_carriers (Origin,Dest,carrier,Avg_Arr_Delay) values ('"+Origin +"','"+Dest+"','"+carrier+"',"+str(Avg_Arr_Delay)+")")	
