#!/usr/bin/python

from operator import itemgetter
import sys

count = 0
Carrier_count = 0
output_dic_1 = {}
output_dic_2 = {}
final_dic = {}
airports_list = []
for row in sys.stdin:
        row = row.strip()
        airport , Carrier , Dep_Delay = row.split(',')
        airport_Carrier = airport+'_'+Carrier
        if Dep_Delay :
                Dep_Delay = float(Dep_Delay)
                output_dic_1[airport_Carrier] = output_dic_1.get(airport_Carrier , 0) + Dep_Delay


for airport_carrier in output_dic_1.keys():
        airport = airport_carrier.split('_')[0]
        if airport not in airports_list :
                airports_list.append(airport)
                for key , value in output_dic_1.items() :
                        if airport == key.split('_')[0]:
                                output_dic_2[key.split('_')[1]] = value
                #print "***** "+airport+" *******"
                count = 0
                for key , value in sorted(output_dic_2.items() , key=itemgetter(1) , reverse=True):
                        print airport +":"+ "%s:%s" % (key , value)
                        count += 1
                        if count == 10:
                                output_dic_2 = {}
                                break
                output_dic_2 = {}
