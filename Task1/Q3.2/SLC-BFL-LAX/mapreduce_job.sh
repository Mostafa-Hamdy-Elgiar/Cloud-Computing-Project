/root/Hadoop/hadoop-2.7.3/bin/hadoop jar /root/Hadoop/hadoop-2.7.3/share/hadoop/tools/lib/hadoop-streaming-2.7.3.jar \
 -input /TransData/MyData.csv \
 -output /Q3.2/SLC-BFL-LAX \
 -file /home/ubuntu/Task1/Q3.2/SLC-BFL-LAX/mapper.py /home/ubuntu/Task1/Q3.2/SLC-BFL-LAX/reducer.py \
 -mapper "/usr/bin/python mapper.py" \
 -reducer "/usr/bin/python reducer.py"
