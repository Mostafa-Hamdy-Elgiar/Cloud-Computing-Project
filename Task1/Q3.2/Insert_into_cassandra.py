#!/usr/bin/python

from cassandra.cluster import Cluster
import os

cluster = Cluster(['127.0.0.1'])
session = cluster.connect()
session.set_keyspace('cloudproject')

airports = ['CMI-ORD-LAX' , 'DFW-ORD-DFW' , 'JAX-DFW-CRP' , 'LAX-ORD-JFK' , 'LAX-SFO-PHX' , 'SLC-BFL-LAX']
for path in airports :
        HDFS_Data = os.popen('/root/Hadoop/hadoop-2.7.3/bin/hadoop fs -cat hdfs://namenode:9000/Q3.2/'+path+'/part-00000')
        HDFS_Data = HDFS_Data.readlines()
        print HDFS_Data

        for c in HDFS_Data :
                airports_path = c.split(':')[0]
                Flights = c.split(':')[1].strip()
                session.execute("insert into Best_paths (airports_path,flights) values ('"+airports_path +"','"+Flights+"')")
