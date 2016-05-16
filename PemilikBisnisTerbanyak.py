from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("PemilikBisnisTerbanyak")
sc = SparkContext(conf=conf)

def parseLine(line):
	fields = line.split(',')
	ownerName = fields[2]
	justHelping = 1
	return (justHelping, ownerName)

lines = sc.textFile('bigdata/testdata.csv')
parsedLines = lines.map(parseLine)
# cobaBalik = parsedLines.map(lambda (x,y) :(x[1], x[0]))
ampas = parsedLines.map(lambda x: (x,1) )
print ampas
results = ampas.collect()



for result in results :
	print result[0] + "Memiliki bisnis sebanyak " + result[1]

