#!/usr/bin/python

from pyspark import SparkContext
from cassandra.cluster import Cluster
import os

cluster = Cluster(['127.0.0.1'])
session = cluster.connect()
session.set_keyspace('cloudproject')

sc=SparkContext()
raw_text = sc.textFile('hdfs://namenode:9000/Q2.1/part-00000' , 4)
r1=raw_text.map(lambda x: x.split(':'))
rdata = r1.collect()

for r in rdata :
        print r[0]
        session.execute("insert into airport_top_carrier (airport,carrier,Dep_Delay) values ('"+r[0] +"','"+r[1]+"',"+str(float(r[2]))+")")
