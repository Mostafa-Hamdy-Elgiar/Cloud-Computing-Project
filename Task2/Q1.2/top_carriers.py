from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: script.py <zk> <topic>", file=sys.stderr)
        exit(-1)

    zkQuorum, topic = sys.argv[1:]

    sc = SparkContext(appName="KafkaSparkStreaming")
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 1)
    ssc.checkpoint("/tmp/q12") # mandatory for updateStateByKey

    ks = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 21})

    def updateFunction(nums, current):
        sumDelay = 0.0
        nrDelay = 0

        for delay in nums:
            sumDelay += delay[0]
            nrDelay += delay[1]

        c0 = current[0] if current is not None else 0.0
        c1 = current[1] if current is not None else 0

        return (sumDelay + c0, nrDelay + c1)

    def output(rdd):
        print('---------------------------------------------')
        carriers = rdd.takeOrdered(10, key = lambda x: x[1][0]/x[1][1])
        for carrier in carriers:
            print("['"+str(carrier[0])+"', "+str(carrier[1][0]/carrier[1][1])+"]")

    st1 = ks.map(lambda x: x[1].split(',')).map(lambda x: (x[1],(float(x[8]),1)) if x[8] else ('0',0)).filter(lambda x: x[0]!='0').updateStateByKey(updateFunction)

    st1.foreachRDD(lambda rdd: output(rdd))

    ssc.start()
    ssc.awaitTermination()
