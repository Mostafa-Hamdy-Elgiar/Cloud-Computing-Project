/root/Hadoop/hadoop-2.7.3/bin/hadoop jar /root/Hadoop/hadoop-2.7.3/share/hadoop/tools/lib/hadoop-streaming-2.7.3.jar \
 -input /TransData/MyData.csv \
 -output /Q2.2 \
 -file /home/ubuntu/Task1/Q2.2/Top_airports/mapper_opt.py /home/ubuntu/Task1/Q2.2/Top_airports/reducer_opt.py \
 -mapper "/usr/bin/python mapper_opt.py" \
 -reducer "/usr/bin/python reducer_opt.py"
