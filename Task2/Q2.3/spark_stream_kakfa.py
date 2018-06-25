from __future__ import print_function
from operator import itemgetter,add
import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from cassandra.cluster import Cluster
from pyspark_cassandra import streaming

def updateFunction(newvalue , lastvalue):
        sumdelay = 0.0
        numdelay = 0
        lsumdelay = 0.0
        lnumdelay = 0
        if lastvalue is not None:
                lsumdelay = lastvalue[0]
                lnumdelay = lastvalue[1]
        for delay in newvalue:
                sumdelay +=delay[0]
                numdelay +=delay[1]
        return (sumdelay + lsumdelay , numdelay + lnumdelay)

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
    if len(sys.argv) != 5:
        print("Usage: script.py <zk> <topic> <source> <destination>", file=sys.stderr)
        exit(-1)

    zkQuorum, topic, origin, dest = sys.argv[1:]

    sc = SparkContext(appName="KafkaSparkStreaming")
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 1)
    ssc.checkpoint("/tmp/q23") # mandatory for updateStateByKey

    ks = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 21})


    group = []
    st1 = ks.map(lambda x: x[1].split(','))
    st2 = st1.map(lambda x: (x[1],(float(x[8]),1)) if x[8] and str(x[3])==origin and str(x[4])==dest else ('0',0)).filter(lambda x: x[0]!='0')
    st3 = st2.reduceByKey(lambda x,y:x+y)
    st4 = st3.map(lambda x:(x[0],x[1])).updateStateByKey(updateFunction)
    st5 = st4.map(lambda (key , values): (key, float(values[0])/values[1]))
    st6 = st5.map(lambda (key , value): (origin+'-'+dest,[[key,value]]))
    st7 = st6.reduceByKey(gettop10).mapValues(addIndex)
    st8 = st7.flatMapValues(lambda x: x)
    #st9 = st8.map(lambda (origin+'-'+dest , (carrier,delay)): {"origin":origin , "dest":dest , "delay":carrier})
    st7.pprint()


    ssc.start()
    ssc.awaitTermination()