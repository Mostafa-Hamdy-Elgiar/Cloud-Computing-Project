from __future__ import print_function

import sys
from operator import itemgetter,add


from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession, SQLContext

start_dates = ['2008-04-03','2008-09-07','2008-01-24','2008-05-16']
second_dates = ['2008-04-05','2008-09-09','2008-01-26','2008-05-18']
origin_list = ['BOS','PHX','DFW','LAX']
conn_list = ['ATL','JFK','STL','MIA']
dest_list = ['LAX','MSP','ORD','LAX']


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: script.py <zk> <topic>", file=sys.stderr)
        exit(-1)

    zkQuorum, topic = sys.argv[1:]

    sc = SparkContext(appName="KafkaSparkStreaming")
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 1)
    ssc.checkpoint("/tmp/q32") # mandatory for updateStateByKey

    ks = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 21})

    def convertdate(st2date):
        st2date_split = st2date.split('-')
        if int(st2date_split[2]) < 12 :
                day = '0'+str(int(st2date_split[2])-2)
        else :
                day = str(int(st2date_split[2])-2)
        newdate = st2date_split[0]+"-"+st2date_split[1]+"-"+day
        return newdate

    def getbest(el1 , el2):
        group = []
        if el2 is None :
                pass
                group = el1
        else :
                group = add(el1, el2)
                group.sort()
        if group :
                group = [group[0]]
        return group

    def getbestone(newvalue,lastvalue):
        new_list = []
        if lastvalue == None :
                new_list = newvalue
        else :
                new_list=add(newvalue,lastvalue)
        new_list.sort()
        return new_list
    def SaveResult(record):
        if record.count() == 0 :
                return
        final_list = []
        res = record.collect()
        for i in res :
                origin = i[0][0]
                dest = i[0][1]
                flight_date = i[0][2]
                arr_delay = i[1][0][0]
                dic_out = {"origin":origin , "dest":dest , "flight_date":flight_date , "arr_delay":arr_delay}
                final_list.append(dic_out)
        spark = SparkSession.builder.master("local").appName("KafkaSparkStreaming").getOrCreate()
        df = spark.createDataFrame(final_list)
        df.write.format("org.apache.spark.sql.cassandra").mode('overwrite').option('confirm.truncate',True).options(table="tom_best_paths", keyspace="cloudproject").save()


    st1 = ks.map(lambda x: x[1].split(',')).map(lambda x: ((x[3],x[4],x[0],x[2]),(float(x[8]),int(x[5]))) if x[8]  else ('0',0)).filter(lambda x: x[0]!='0')
    path1 = st1.map(lambda (key , value): ((key[1],key[2]),[(key[0],key[1],key[3],value[0],value[1],key[2],'AM')]) if value[1] < 1200  else ('0',0)).filter(lambda x: x[0]!='0' and x[0][0] in conn_list and x[0][1] in start_dates and x[1][0][0] in origin_list)
    path2 = st1.map(lambda (key , value): ((key[0],convertdate(key[2])),[(key[0],key[1],key[3],value[0],value[1],key[2],'PM')]) if value[1] > 1200 else ('0',0)).filter(lambda x: x[0]!='0' and x[0][0] in conn_list and x[0][1] in start_dates and x[1][0][1] in dest_list)
    st3 = path1.union(path2)
    st4 = st3.map(lambda (key,value): ((value[0][0],value[0][1],value[0][5],value[0][6]),[value[0][3]]))
    st5 = st4.reduceByKey(getbest).updateStateByKey(getbestone)
    st6 = st5.foreachRDD(SaveResult)

    ssc.start()
    ssc.awaitTermination()
