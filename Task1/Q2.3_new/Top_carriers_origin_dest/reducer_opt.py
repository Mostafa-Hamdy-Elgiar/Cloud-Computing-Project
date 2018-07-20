#!/usr/bin/python

from operator import itemgetter
import sys

count = 0
output_dic = {}
output_dic_count = {}
output_dic_2 = {}
final_dic = {}
airports_list = []
for row in sys.stdin:
        row = row.strip()
        origin , dest , Carrier , Arr_Delay = row.split(',')
        origin_dest_carrier = origin+'_'+dest+'_'+Carrier
        if Arr_Delay :
                Arr_Delay = float(Arr_Delay)
                output_dic[origin_dest_carrier] = output_dic.get(origin_dest_carrier , 0) + Arr_Delay
                output_dic_count[origin_dest_carrier]= output_dic_count.get(origin_dest_carrier , 0) + 1


for origin_dest_carrier in output_dic.keys():
        origin_dest = origin_dest_carrier.split('_')[0] + '_'+origin_dest_carrier.split('_')[1]
        if origin_dest not in airports_list:
                airports_list.append(origin_dest)
                for key , value in output_dic.items() :
                        if origin_dest == key.split('_')[0] +'_'+ key.split('_')[1] :
                                AVG_value= value / output_dic_count[origin_dest_carrier]
                                output_dic_2[key.split('_')[2]] = AVG_value

                #print output_dic_2
                #print "**** "+origin_dest+" *****"
                count = 0
                for key , value in sorted(output_dic_2.items() , key=itemgetter(1) , reverse=True):
                        print origin_dest+':'+"%s:%s" % (key , value)
                        count += 1
                        if count == 10:
                                output_dic_2 = {}
                                break
                output_dic_2 = {}
