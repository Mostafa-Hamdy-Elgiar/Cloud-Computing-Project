from __future__ import print_function

from operator import itemgetter,add
import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from cassandra.cluster import Cluster
from pyspark_cassandra import streaming

def updateFunction(newvalue , lastsum):
        if lastsum is None :
                lastsum = 0
        return sum(newvalue , lastsum)

def gettop10(group , element):
        group = add(group , element)
        group.sort(key=itemgetter(1) , reverse=True)
        if len(group) > 10:
                group = group[:10]
        return group

def addIndex(group):
        for i, line in enumerate(group):
                line.insert(0,i+1)
        return group


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: script.py <zk> <topic> <origin_aitport>", file=sys.stderr)
        exit(-1)

    zkQuorum, topic, origin = sys.argv[1:]

    sc = SparkContext(appName="KafkaSparkStreaming")
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 1)
    ssc.checkpoint("/tmp/q22") # mandatory for updateStateByKey
    ks = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 21})


    st1 = ks.map(lambda x: x[1].split(','))
    st2 = st1.map(lambda x: (x[4],float(x[6])) if x[6] and str(x[3])==origin else ('0',0))
    st3 = st2.reduceByKey(lambda x,y:x+y)
    st4 = st3.map(lambda x:(x[0],x[1])).updateStateByKey(updateFunction)
    st5 = st4.map(lambda (key , value): (origin,[[key,value]]))
    st6 = st5.reduceByKey(gettop10).mapValues(addIndex)
    st7 = st6.flatMapValues(lambda x: x)
    st8 = st7.map(lambda (origin , (dest,delay)): {"origin":origin , "dest":dest , "delay":delay})
    #st8.saveToCassandra("cloudproject" , 'testtable')
    st7.pprint()
    #cluster = Cluster(['127.0.0.1'])
    #session = cluster.connect()
    #session.set_keyspace('cloudproject')

    ssc.start()
    ssc.awaitTermination()
