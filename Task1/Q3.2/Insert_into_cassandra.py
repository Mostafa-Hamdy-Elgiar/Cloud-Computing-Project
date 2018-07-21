#!/usr/bin/python

from pyspark import SparkContext
from cassandra.cluster import Cluster
import os

cluster = Cluster(['127.0.0.1'])
session = cluster.connect()
session.set_keyspace('cloudproject')
sc=SparkContext()

airports = ['CMI-ORD-LAX' , 'DFW-ORD-DFW' , 'JAX-DFW-CRP' , 'LAX-ORD-JFK' , 'LAX-SFO-PHX' , 'SLC-BFL-LAX']
for path in airports :
        #sc=SparkContext()
        raw_text = sc.textFile('hdfs://namenode:9000/Q3.2/'+path+'/part-00000' , 1)
        r1=raw_text.map(lambda x: x.split(':'))
        rdata = r1.collect()
        session.execute("insert into Best_paths (airports_path,flights) values ('"+rdata[0][0].strip()+"','"+rdata[0][1].strip()+"')")
