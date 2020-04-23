#!/usr/bin/python2
import csv
import fileinput
import sys
from collections import namedtuple


StationInfo = namedtuple('StationInfo', ['lat', 'lon', 'name'])
stations = dict()

# "USAF","WBAN","STATION NAME","CTRY","STATE","ICAO","LAT","LON","ELEV(M)","BEGIN","END"
with open('isd-history.csv', 'rb') as stations_file:
	reader = csv.DictReader(stations_file)
	for row in reader:
		if row['USAF'] != '999999' and row['LAT'] and row['LON']:
			station_id = int(row['USAF'])
			inf = StationInfo(float(row['LAT']), float(row['LON']), row['STATION NAME'])
			stations[station_id] = inf


# 100001:09.03	21.0C (2014)
# 100010:09.03	8.0C (1992)
# 100020:09.03	9.0C (1977)

for line in fileinput.input():
	station_day = line.split()[0]
	data = ' '.join(line.split()[1:])
	s_id = int(line.split(':')[0])
	
	try:
		inf = stations[s_id]
		print '%.4f, %.4f, %s (%s)' % (inf.lat, inf.lon, inf.name, data)
	except KeyError:
		print >> sys.stderr, "Station %d not found" % s_id
