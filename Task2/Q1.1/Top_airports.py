from __future__ import print_function

from operator import itemgetter,add
import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

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
    ssc.checkpoint("/tmp/q22") # mandatory for updateStateByKey

    ks = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 21})


    st1 = ks.map(lambda x: x[1].split(','))
    st2 = st1.flatMap(lambda x: [x[3],x[4]])
    st3= st2.map(lambda x: (x,1))
    st4 = st3.reduceByKey(lambda x,y:x+y)
    st5 = st4.map(lambda x:(x[0],x[1])).updateStateByKey(updateFunction)
    st6 = st5.map(lambda (key , value):(True, [(key,value)]))
    st7 = st6.reduceByKey(gettop10)
    st7.pprint()

    ssc.start()
    ssc.awaitTermination()
