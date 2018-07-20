#!/usr/bin/python

from operator import itemgetter
import sys

count = 0
Carrier_count = 0
output_dic = {}
output_dic_2 = {}
final_dic = {}
airports_list = []
for row in sys.stdin:
        row = row.strip()
        origin_airport , Dest_airport , Dep_Delay = row.split(',')
        origin_dest = origin_airport+'_'+Dest_airport
        if Dep_Delay :
                Dep_Delay = float(Dep_Delay)
                output_dic[origin_dest] = output_dic.get(origin_dest , 0) + Dep_Delay

for origin_dest in output_dic.keys():
        origin = origin_dest.split('_')[0]
        if origin not in airports_list :
                airports_list.append(origin)
                for key , value in output_dic.items():
                        if origin == key.split('_')[0]:
                                output_dic_2[key.split('_')[1]] = value
                count = 0
                for key , value in sorted(output_dic_2.items(), key=itemgetter(1) , reverse=True):
                        print origin+':'+"%s:%s" % (key , value)
                        count += 1
                        if count == 10:
                                output_dic_2 = {}
                                break
                output_dic_2 = {}
