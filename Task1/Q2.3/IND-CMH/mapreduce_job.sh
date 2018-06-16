/root/Hadoop/hadoop-2.7.3/bin/hadoop jar /root/Hadoop/hadoop-2.7.3/share/hadoop/tools/lib/hadoop-streaming-2.7.3.jar \
 -input /TransData/MyData.csv \
 -output /Q2_3/IND-CMH \
 -file /home/ubuntu/Task1/Q5/IND-CMH/mapper_opt.py /home/ubuntu/Task1/Q5/IND-CMH/reducer_opt.py \
 -mapper "/usr/bin/python mapper_opt.py" \
 -reducer "/usr/bin/python reducer_opt.py"
