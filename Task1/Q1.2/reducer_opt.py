#!/usr/bin/python

from operator import itemgetter
import sys

output_dic_1 = {}
output_dic_2 = {}
final_dic = {}
count = 0
for row in sys.stdin:
        row = row.strip()
        Carrier , Delay = row.split(',')
	#print count
	#count +=1
	if Delay :
		count += 1
		Delay = float(Delay)
		output_dic_1[Carrier] = output_dic_1.get(Carrier , 0) +  Delay
		output_dic_2[Carrier] = output_dic_2.get(Carrier , 0) +  1

for Carrier in output_dic_1.keys():
	sum_Del = output_dic_1[Carrier]
	nu_elm = output_dic_2[Carrier]
	AVG_Del = sum_Del / nu_elm
	#print "%s : %s" % (Carrier , AVG_Del)
	final_dic[Carrier] = AVG_Del

count = 0
for key, value in sorted(final_dic.items(),key=itemgetter(1)):
        print "%s: %s" % (key , value)
        count += 1
        if count == 10:
                break
