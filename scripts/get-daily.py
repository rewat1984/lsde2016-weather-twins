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

