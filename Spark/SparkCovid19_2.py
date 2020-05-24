from __future__ import print_function

import findspark
findspark.init()

import sys
import pyspark
from operator import add
import datetime
from pyspark.sql import SparkSession
import csv

import time


if __name__ == "__main__":
    start = time.time()
    if len(sys.argv) != 5:
        print("Number of args not correct", file=sys.stderr)
        sys.exit(-1)

    startdate = datetime.datetime.strptime(sys.argv[2], '%Y-%m-%d').date()
    enddate = datetime.datetime.strptime(sys.argv[3], '%Y-%m-%d').date()

    if enddate < startdate:
        print("End date less than start date")
        sys.exit(-1)
    if (startdate<datetime.datetime.strptime('2019-12-31', '%Y-%m-%d').date() or enddate > datetime.datetime.strptime('2020-04-08', '%Y-%m-%d').date()):
        print("dates not in range")
        sys.exit(-1)


    sc = pyspark.SparkContext('local[*]')
    spark = SparkSession\
        .builder\
        .appName("PythonCovidCount")\
        .getOrCreate()

    filtered_date = sc.textFile(sys.argv[1]) \
        .map(lambda line: line.split(",")) \
        .filter(lambda line: len(line) == 4) \
        .filter(lambda line: line[0] != "date") \
        .filter(lambda line: datetime.datetime.strptime(line[0], '%Y-%m-%d').date() >= startdate
                and datetime.datetime.strptime(line[0], '%Y-%m-%d').date() <= enddate)

    mapped = filtered_date.map(lambda line: (line[1], int(line[3])))
    reduced = mapped.reduceByKey(add)
    output = reduced.collect()
    output = sorted(output)

    write_file = open(sys.argv[4] + "covid19_2_output.csv", 'w', newline='')
    writer = csv.writer(write_file)

    for (word, count) in output:
        print("%s: %i" % (word, count))
        writer.writerow([word, count])
    write_file.close()
    print('It took', time.time() - start, 'seconds.')
    spark.stop()
