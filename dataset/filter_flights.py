"""
To be used with itineraries.csv, it filters the data to only contain
information on the flights happening in August 2022.
"""
import datetime

f = open("itineraries.csv", "r")
f2 = open("sm_itineraries.csv", "w")

print(f.readline())
for idx, line in enumerate(f):
    splitline = line.split(',')
    date1 = splitline[2].split('-')
    month1 = int(date1[1])
    year1 = int(date1[0])
    date2 = splitline[1].split('-')
    month2 = int(date2[1])
    year2 = int(date2[0])
    if month1 == 8 and year1 == 2022:
        f2.write(line)
    elif month2 == 9 and year2 == 2022:
        break
