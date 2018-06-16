#!/usr/bin/python

import matplotlib.pyplot as plt
import math

airports_list = []
airports = open('All_airports_by_rank.txt' , 'r')
Read_airports = airports.readlines()
xcount = 1
xcount_list = []
for i in Read_airports :
	flights = math.log10(int(i.split(': ')[1].rstrip('\n')))
	airports_list.append(flights)
	xcount_list.append(math.log10(xcount))
	xcount += 1

plt.plot(xcount_list , airports_list)
plt.ylabel('Number of flights (log)')
plt.xlabel('Airports ranking (log)')
plt.show()
