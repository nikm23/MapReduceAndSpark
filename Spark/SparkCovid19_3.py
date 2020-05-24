from __future__ import print_function

import findspark
findspark.init()

import sys
import pyspark
from operator import add
import csv
import math
from pyspark.sql import SparkSession
import time
def populationdata(file):
    with open(file) as csvfile:
        readCSV = csv.reader(csvfile, delimiter=',')
        data_dict = dict()
        for row in readCSV:
            if row[4]!="population":
                data_dict[row[0]] = row[4]
        return data_dict

if __name__ == "__main__":
    start = time.time()
    if len(sys.argv) != 4:
        print("Number of args not correct", file=sys.stderr)
        sys.exit(-1)

    sc = pyspark.SparkContext('local[*]')
    spark = SparkSession\
        .builder\
        .appName("PythonCovidCount")\
        .getOrCreate()

    filtered_date = sc.textFile(sys.argv[1]) \
        .map(lambda line: line.split(",")) \
        .filter(lambda line: len(line) == 4) \
        .filter(lambda line: line[0] != "date")

    cache_map = populationdata(sys.argv[2])

    broadcastVar = sc.broadcast(cache_map)
    mapped = filtered_date.map(lambda line: (line[1], (broadcastVar.value.get(line[1]), int(line[2]))))\
        .filter(lambda x: x[1][0]!="" and x[1][0]!=None)
    reduced = mapped.reduceByKey(lambda x,y : (x[0], x[1]+y[1]))
    output = reduced.collect()
    output = sorted(output)

    write_file = open(sys.argv[3] + "covid19_3_output.csv", 'w', newline='')
    writer = csv.writer(write_file)

    for (country, (population, sum_cases)) in output:
        avg = (float(sum_cases)/float(population))*1000000.0
        print(country, avg)
        writer.writerow([country, avg])
    write_file.close()

    print('It took', time.time() - start, 'seconds.')
    spark.stop()
