#!/usr/bin/python

from operator import itemgetter 
import sys

output_dic_1 = {}
output_dic_2 = {}
for row in sys.stdin:
	#print row
	row = row.strip()
	org_dest , dep_time , Arr_delay , flight_number = row.split(',')
	if org_dest == 'CMI-ORD' and dep_time and int(dep_time) < 1200 :
		output_dic_1[flight_number] = float(Arr_delay)
	if org_dest == 'ORD-LAX' and dep_time and  int(dep_time) > 1200 :
		output_dic_2[flight_number] = float(Arr_delay_

output_dic_1 = sorted(output_dic_1.items() , key=itemgetter(1))
output_dic_2 = sorted(output_dic_2.items() , key=itemgetter(1))
print "CMI-ORD-LAX : " + output_dic_1[0][0] +','+output_dic_2[0][0]
