from pyspark import SparkConf, SparkContext
import collections,sys
from pyspark.mllib.clustering import KMeans, KMeansModel
from numpy import array
from math import sqrt
from mpl_toolkits.basemap import Basemap
import matplotlib.pyplot as plt
import csv


def parseOwnership(line):
    fields = line.split(',')
    #print len(fields)
    owner = fields[0]
    taxes = fields[1]
    lat = fields[2]
    lon = fields[3]
    cluster = fields[4]
    
    return (owner, taxes, lat, lon, cluster)

conf = SparkConf().setMaster("local").setAppName("tugasbigdata")
sc = SparkContext(conf = conf)

lines = sc.textFile("file:///SparkCourse/OwnershipData.csv")
parsedLines = lines.map(parseOwnership)


taxData = parsedLines.map(lambda x: (x[4], int(x[1])))
total_tax = taxData.reduceByKey(lambda x,y : x+y)
countBs = parsedLines.map(lambda x : (x[4], int(1)))
total_business = countBs.reduceByKey(lambda x,y : x+y)

taxs = total_tax.collect()
counts = total_business.collect();

for i in range(len(taxs)):
    total_taxs = float(taxs[i][1])
    total_bs = float(counts[i][1])
    print 'Cluster %d: %d %.2f' % (int(taxs[i][0]), int(counts[i][1]), total_taxs/total_bs)
    
allData = parsedLines.collect()

id = 1
business_mapper={"Dummy":1}
for i in range(len(allData)):
    if(not business_mapper.has_key(allData[i][0])):
        business_mapper[allData[i][0]]=id
        id = id+1

clust1 = [0]*id
clust2 = [0]*id
clust0 = [0]*id
count_bs = [0]*id
total_tx = [0]*id
mean_tx = [0]*id

distract = 0

for i in range(len(allData)):
    idx = business_mapper[allData[i][0]]
    if (allData[i][4]==0):
        clust0[idx] = clust0[idx]+1
    elif (allData[i][4]==1):
        clust1[idx] = clust1[idx]+1
    else:
        clust2[idx] = clust2[idx]+1
    count_bs[idx] = count_bs[idx]+1
    total_tx[idx] = total_tx[idx]+int(allData[i][1])

print max(count_bs)
total_business = [0]*100
total_taxes = [0]*100
for key,value in business_mapper.iteritems():
    #print '%s c0:%d, c1:%d, c2:%d, count:%d' % (key, clust0[value], clust1[value], clust2[value], count_bs[value])
    mean_tx[value] = float(total_tx[value]) / float(count_bs[value])
    total_taxes[count_bs[value]] = total_taxes[count_bs[value]] + mean_tx[value]
    total_business[count_bs[value]] = total_business[count_bs[value]] + 1
    a=clust0[value]*clust1[value]
    b=clust0[value]*clust2[value]
    c=clust1[value]*clust2[value]
    if(a>0 or b>0 or c>0):
        distract = distract+1

print 'Total Business : %d' % (len(allData))
print 'Distributed Business : %d' % (distract)

for i in range(100):
    if (total_business[i] != 0):
        mean_tax = float(total_taxes[i]) / float(total_business[i])
        print 'Num. of Business : %d, count : %d, mean tax : %.2f' % (i, total_business[i], mean_tax)
        