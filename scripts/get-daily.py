import sys
import time
import math
import utils
from pyspark.context import SparkContext

hdfs_file_path = "/user/lsde02/data/1901/*.gz"
hdfs_results_path = "/user/lsde02/results/"

sc = SparkContext()
context = sc.textFile(hdfs_file_path)
stations = context.flatMap(lambda x: [utils.extract(record) for record in x.splitlines()])
stations = stations.filter(lambda x: 'longitude' in x[1] and 'latitude' in x[1])

# Do computations on month level
# Compute monthly temperatures
temp_month_avg = utils.noaa_month_average(stations, 'temp')
temp_month_avg = temp_month_avg.coalesce(1, True)
temp_month_avg.saveAsTextFile("temp-%s%s" % (hdfs_results_path, time.strftime("%Y-%m-%d-%H-%M-%S")))
# Compute monthly wind speeds
speed_month_avg = utils.noaa_month_average(stations, 'wind-speed')
speed_month_avg = speed_month_avg.coalesce(1, True)
temp_month_avg.saveAsTextFile("windspeed-%s%s" % (hdfs_results_path, time.strftime("%Y-%m-%d-%H-%M-%S")))
# Compute sky condition 
month_avg = utils.noaa_month_average(stations, 'sky-condition')
month_avg = month_avg.coalesce(1, True)
month_avg.saveAsTextFile("sky-condition-%s%s" % (hdfs_results_path, time.strftime("%Y-%m-%d-%H-%M-%S")))
# Compute visibility
month_avg = utils.noaa_month_average(stations, 'visibility')
month_avg = month_avg.coalesce(1, True)
month_avg.saveAsTextFile("visibility-%s%s" % (hdfs_results_path, time.strftime("%Y-%m-%d-%H-%M-%S")))

