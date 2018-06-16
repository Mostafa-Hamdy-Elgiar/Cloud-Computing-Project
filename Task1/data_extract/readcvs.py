#!/usr/bin/python

import csv,os

count = 0
os.chdir("work/airline_ontime")
files = os.popen("find . -name *.zip")
for f in files:
        print f
        file_name = f.split("/")[-1]
        os.system("rm readme.html")
        os.system("unzip "+f)
        count = 1

os.chdir("/srv/work")
csv_files = os.popen("find . -name *.csv")
CSV_new = open('/srv/MyData.csv' , 'wb')
for csv_f in csv_files :
        open_csv = open(csv_f.rstrip("\n") , 'rb')
        read_csv = csv.reader(open_csv , delimiter=',')
        count = 0
        for row in read_csv :
                if count == 0 :
                        count +=1
                        ### Required fields ###
                        FlightDate_index = row.index('FlightDate')
                        UniqueCarrier_index = row.index('UniqueCarrier')
                        FlightNum_index = row.index('FlightNum')
                        Origin_index = row.index('Origin')
                        Dest_index = row.index('Dest')
                        DepTime_index = row.index('DepTime')
                        DepDelay_index = row.index('DepDelay')
                        ArrTime_index = row.index('ArrTime')
                        ArrDelayMinutes_index = row.index('ArrDelayMinutes')
                else :
                        FlightDate = row[FlightDate_index]
                        UniqueCarrier = row[UniqueCarrier_index]
                        FlightNum = row[FlightNum_index]
                        Origin = row[Origin_index]
                        Dest = row[Dest_index]
                        DepTime = row[DepTime_index]
                        DepDelay = row[DepDelay_index]
                        ArrTime = row[ArrTime_index]
                        ArrDelayMinutes = row[ArrDelayMinutes_index]

                        ### Write This data in new CSV file ###
                        Write_data = csv.writer(CSV_new , delimiter=',')
                        Write_data.writerow([FlightDate , UniqueCarrier , FlightNum , Origin , Dest , DepTime , DepDelay , ArrTime , ArrDelayMinutes])
