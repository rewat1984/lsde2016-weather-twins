import sys
import time
import math
import utils
from pyspark.context import SparkContext

if (len(sys.argv) > 1):
	hdfs_file_path = "/user/lsde02/data/%s/*.gz" % sys.argv[1]
else:
	hdfs_file_path = "/user/lsde02/data/1901/*.gz"
hdfs_results_path = "/user/lsde02/results/"
start_time = time.strftime("%Y-%m-%d-%H-%M-%S")

sc = SparkContext()
context = sc.textFile(hdfs_file_path)
stations = context.flatMap(lambda x: [utils.extract(record) for record in x.splitlines()])
stations = stations.filter(lambda x: 'longitude' in x[1] and 'latitude' in x[1])
stations.persist()

# Do computations on month level
month_data = stations.map(lambda x:((x[0][0], x[0][1], x[0][3]), (x[1]['temp'], x[1]['wind-speed'], x[1]['sky-condition'], x[1]['visibility'], \
				x[1]['wind-direction'])))
month_data = month_data.combineByKey(lambda value: (x['temp'], 1, x['wind-speed'], 1, x['sky-condition'], 1, x['visibility'], 1, \
				math.sin(x['wind-direction'])*math.pi/180., math.cos(x['wind-direction']*math.pi/180.)),\
				lambda x, value: (x[0] + value[0], value[1] + 1, x[2]+value[2], 1 + value[3], x[4] + value[4], 1 + value[5],\
					x[6]+value[6], 1 + value[7], x[8] + value[8], x[9] + value[9]),\
				lambda x, y: (x[0]+y[0], x[1]+y[1], x[2]+y[2], x[3]+y[3], x[4]+y[4], x[5]+y[5], x[6]+y[6], x[7]+y[7], x[8]+y[8]\
					x[9]+y[9])) 
month_data = month_data.map(lambda (label, (x1, c1, x2, c2, x3, c3, x4, c4, x5a, x5b)): (label, (x1/c1, x2/c2, x3/c3, x4/c4, math.atan2(x5a, x5b))))
month_data = month_data.coalesce(1, True)
month_avg.saveAsTextFile("%s%s-%s" % (hdfs_results_path, start_time, 'all'))
'''
# Compute monthly temperatures
temp_month_avg = utils.noaa_month_average(stations, 'temp')
temp_month_avg = temp_month_avg.coalesce(1, True)
temp_month_avg.saveAsTextFile("%s%s-%s" % (hdfs_results_path, start_time, 'temp'))
# Compute monthly wind speeds
speed_month_avg = utils.noaa_month_average(stations, 'wind-speed')
speed_month_avg = speed_month_avg.coalesce(1, True)
temp_month_avg.saveAsTextFile("%s%s-%s" % (hdfs_results_path, start_time, 'wind-speed'))
# Compute sky condition
month_avg = utils.noaa_month_average(stations, 'sky-condition')
month_avg = month_avg.coalesce(1, True)
month_avg.saveAsTextFile("%s%s-%s" % (hdfs_results_path, start_time, 'sky-condition'))
# Compute visibility
month_avg = utils.noaa_month_average(stations, 'visibility')
month_avg = month_avg.coalesce(1, True)
month_avg.saveAsTextFile("%s%s-%s" % (hdfs_results_path, start_time, 'visibility'))
# Compute wind direction
month_avg = utils.noaa_circ_average(stations, 'wind-direction')
month_avg = month_avg.coalesce(1, True)
month_avg.saveAsTextFile("%s%s-%s" % (hdfs_results_path, start_time, 'wind-direction'))
'''

