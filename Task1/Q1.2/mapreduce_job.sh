/root/Hadoop/hadoop-2.7.3/bin/hadoop jar /root/Hadoop/hadoop-2.7.3/share/hadoop/tools/lib/hadoop-streaming-2.7.3.jar \
 -input /TransData/MyData.csv \
 -output /Most_popular_airlines \
 -file /home/ubuntu/Task1/Q2/mapper.py /home/ubuntu/Task1/Q2/reducer_opt.py \
 -mapper "/usr/bin/python mapper.py" \
 -reducer "/usr/bin/python reducer_opt.py"
