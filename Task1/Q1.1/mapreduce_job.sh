/root/Hadoop/hadoop-2.7.3/bin/hadoop jar /root/Hadoop/hadoop-2.7.3/share/hadoop/tools/lib/hadoop-streaming-2.7.3.jar \
 -input /TransData/MyData.csv \
 -output /Q1.1 \
 -file /home/ubuntu/mapper.py /home/ubuntu/reducer.py \
 -mapper "/usr/bin/python mapper.py" \
 -reducer "/usr/bin/python reducer.py"
