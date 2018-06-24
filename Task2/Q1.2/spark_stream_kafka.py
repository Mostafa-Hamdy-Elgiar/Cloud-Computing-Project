from __future__ import print_function

from operator import itemgetter,add
import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from cassandra.cluster import Cluster
from pyspark_cassandra import streaming

def updateFunction(newvalue , lastvalue):
        arrdelay = 0.0
        delaycount = 0
        lastdelay = lastvalue[0] if lastvalue is not None else 0.0
        lastcount = lastvalue[1] if lastvalue is not None else 0
        for i in newvalue :
                arrdelay += i[0]
                delaycount +=i[1]
        return (arrdelay+lastdelay , delaycount+lastcount)

def gettop10(group,element):
        group = add(group , element)
        group.sort(key=itemgetter(1))
        if len(group) > 10:
                group = group[:10]
        return group
        
        
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: script.py <zk> <topic>", file=sys.stderr)
        exit(-1)

    zkQuorum, topic = sys.argv[1:]

    sc = SparkContext(appName="KafkaSparkStreaming")
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 1)
    ssc.checkpoint("/tmp/q1.2") # mandatory for updateStateByKey

    ks = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 21})


    st1 = ks.map(lambda x: x[1].split(','))
    st2 = st1.map(lambda x: (x[1],float(x[8])) if x[8] else ('0',0)).filter(lambda x: x[0]!='0')
    st3 = st2.reduceByKey(lambda x,y:x+y)
    st4 = st3.map(lambda x:(x[0],(x[1],1))).updateStateByKey(updateFunction)
    st5 = st4.map(lambda (key , value):(True, [(key,value[0]/value[1])]))
    #st6 = st5.reduceByKey(gettop10).mapValues(addIndex)
    st6 = st5.reduceByKey(gettop10)
    #st7 = st6.flatMapValues(lambda x: x)
    #st8 = st7.map(lambda ( True, (carrier,delay)): {"carrier":carrier, "arr_delay":delay})
    #st8.saveToCassandra("cloudproject" , 'testtable')
    st6.pprint()

    ssc.start()
    ssc.awaitTermination()
                                                                                                                             64,1          Bot
