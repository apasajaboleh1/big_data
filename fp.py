from pyspark import SparkConf, SparkContext
import collections
import sys

conf = SparkConf().setMaster("local").setAppName("tugasbigdata")
sc = SparkContext(conf = conf)

def parseLine(line):
    data_field=line.split(",")
    nama_univ=data_field[1]
    negara_univ=data_field[2]
    ranking_that_year=[]
    ranking_that_year.append(data_field[0],data_field[13])
    return (nama_univ,negara_univ,ranking_that_year)


orig_stdout = sys.stdout
f = file('test_dat.txt', 'w')
sys.stdout = f

lines = sc.textFile("file:///SparkCourse/cwurData.csv")
parsedLines = lines.map(parseLine)

ambil_test=parsedLines.collect()
for a in ambil_test:
    print a
sys.stdout = orig_stdout
f.close()
