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

def gettop10(group,element):
        group = add(group , element)
        group.sort(key=itemgetter(1),reverse=True)
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
    ssc.checkpoint("/tmp/q1.1") # mandatory for updateStateByKey

    ks = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 21})


    st1 = ks.map(lambda x: x[1].split(','))
    st2 = st1.map(lambda x: (x[3],1))
    st3 = st2.reduceByKey(lambda x,y:x+y)
    st4 = st3.map(lambda x:(x[0],x[1])).updateStateByKey(updateFunction)
    st5 = st4.map(lambda (key , value):(True, [(key,value)]))
    st6 = st5.reduceByKey(gettop10)
    #st7 = st6.flatMapValues(lambda x: x)
    #st8 = st7.map(lambda (True , (airport,count)): {"airport":airport , "flights":count})
    #st8.saveToCassandra("cloudproject" , 'testtable')
    st6.pprint()

    ssc.start()
    ssc.awaitTermination()
