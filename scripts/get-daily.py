import sys
import time
import math
import utils

hdfs_file_path = "/user/lsde02/data/1901/*.gz"
hdfs_results_path = "/user/lsde02/results/"

context = sc.textFile(hdfs_file_path)
stations = context.flatMap(lambda x: [utils.extract(record) for record in x.splitlines()])
stations = stations.filter(lambda x: 'longitude' in x[1] and 'longitude' in x[1])

# Filter all records that contain invalid temp or have a low quality label
temp = stations.filter(lambda x: (x[1]['temp']!='+9999' and x[1]['temp-q']!='3' and x[1]['temp-q']!='7'))
temp = temp.map(lambda x: (x[0], int(int(x[1]['temp'])/100.0)))

# Compute daily averages
temp_month = temp.combineByKey(lambda value: (value, 1),\
				lambda x, value: (x[0] + value, x[1] + 1),\
				lambda x, y: (x[0] + y[0], x[1] + y[1]))
temp_daily_avg = temp_month.map(lambda (label, (value_sum, count)): (label, float(value_sum)/count))

# Compute monthly averages
temp_month_avg = temp_daily_avg.map(lambda x: ((x[0][0], x[0][1],x[0][3]), x[1]))
temp_month_avg = temp_month_avg.combineByKey(lambda value: (value, 1),\
                                lambda x, value: (x[0] + value, x[1] + 1),\
                                lambda x, y: (x[0] + y[0], x[1] + y[1]))
temp_month_avg = temp_month_avg.map(lambda (label, (value_sum, count)): (label, float(value_sum)/count))
temp_month_avg = temp_month_avg.coalesce(1, True)

# Save results to file
temp_month_avg.saveAsTextFile("%s%s" % (hdfs_results_path ,time.strftime("%Y-%m-%d-%H-%M-%S")))
