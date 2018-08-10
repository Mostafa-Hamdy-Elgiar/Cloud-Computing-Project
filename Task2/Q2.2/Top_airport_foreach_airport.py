from __future__ import print_function

import sys
from operator import itemgetter,add


from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession, SQLContext

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

    def updateFunction(nums, current):
        sumDelay = 0.0
        nrDelay = 0

        for delay in nums:
            sumDelay += delay[0]
            nrDelay += delay[1]

        c0 = current[0] if current is not None else 0.0
        c1 = current[1] if current is not None else 0

        return (sumDelay + c0, nrDelay + c1)


    def gettop10(group , element):
        group = add(group , element)
        group.sort(key=itemgetter(1))
        if len(group) > 10:
                group = group[:10]
        return group

    def SaveResult(record):
        if record.count() == 0 :
                return
        final_list = []
        second_list = []
        res = record.collect()
        for i in res :
                airport = i[0]
                for ii in i[1] :
                        dest = ii[0]
                        dep_delay = ii[1][0]
                        number = ii[1][1]
                        avg_delay = dep_delay/number
                        dic_out = {"origin":airport , "dest":dest , "dep_delay":avg_delay}
                        final_list.append(dic_out)
        spark = SparkSession.builder.master("local").appName("KafkaSparkStreaming").getOrCreate()
        df = spark.createDataFrame(final_list)
        df.write.format("org.apache.spark.sql.cassandra").mode('overwrite').option('confirm.truncate',True).options(table="airport_top_airports", keyspace="cloudproject").save()


    st1 = ks.map(lambda x: x[1].split(',')).map(lambda x: ((x[3],x[4]),(float(x[6]),1)) if x[6] else ('0',0)).filter(lambda x: x[0]!='0').updateStateByKey(updateFunction)
    st2 = st1.map(lambda (key , value): (key[0] , [(key[1] , value)])).reduceByKey(gettop10)
    st3 = st2.foreachRDD(SaveResult)

    ssc.start()
    ssc.awaitTermination()
