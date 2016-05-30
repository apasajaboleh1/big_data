from pyspark import SparkConf, SparkContext
import collections,sys
from pyspark.mllib.clustering import KMeans, KMeansModel
from numpy import array
from math import sqrt
from mpl_toolkits.basemap import Basemap
import matplotlib.pyplot as plt
import csv

def parseLine(line):
    fields = line.split(',')
    #print len(fields)
    if len(fields) >18 :
        city = fields[5]
        latitude= None
        logitude =None
        templatitude = fields[17].split('"')
        #if len(templatitude)==2:
        temp2= templatitude[0]
        if temp2!='':
            temp3=temp2.split()
            if len(temp3)<2:
                latitude =temp2
        templogitude = fields[18].split('"')
        temp21=templogitude[0]
        if temp21 !='' :
            temp31=temp21.split()
            if len(temp31)<2 :
                logitude =temp21
        
        #print bisnis_id,state,latitude,logitude
        return (city,latitude,logitude)
        
def parseOwner(line):
    fields = line.split(',')
    #print len(fields)
    if len(fields) >18 :
        city = fields[5]
        latitude= None
        logitude =None
        templatitude = fields[17].split('"')
        #if len(templatitude)==2:
        temp2= templatitude[0]
        if temp2!='':
            temp3=temp2.split()
            if len(temp3)<2:
                latitude =temp2
        templogitude = fields[18].split('"')
        temp21=templogitude[0]
        if temp21 !='' :
            temp31=temp21.split()
            if len(temp31)<2 :
                logitude =temp21
        
        owners = fields[2]
        taxes = fields[12]
        #print bisnis_id,state,latitude,logitude
        return (city,owners,taxes,latitude,logitude)
        
def parseOwnership(line):
    fields = line.split(',')
    #print len(fields)
    owner = fields[0]
    taxes = int(fields[1])
    lat = float(fields[2])
    lon = float(fields[3])
    cluster = int(fields[4])
    
    return (owner, taxes, lat, lon, cluster)

conf = SparkConf().setMaster("local").setAppName("tugasbigdata")
sc = SparkContext(conf = conf)

clusters = KMeansModel.load(sc,"C:/SparkCourse/FP_model")

lines = sc.textFile("file:///SparkCourse/data_center.csv")
parsedLines = lines.map(parseLine)
#bersih bersih
reserveddata= parsedLines.filter(lambda x : x is not None)
reserveddata1= reserveddata.filter(lambda x : x[1] is not None)
reserveddata2=reserveddata1.filter(lambda x : x[2] is not None)
temp=reserveddata2.filter(lambda x : "San Francisco" in x[0])
tempdata=temp.map(lambda x: (x[1],x[2]))
data=tempdata.map(lambda x: (float(x[0]),float(x[1])))

data_local = data.collect()

ownershipData = lines.map(parseOwner)
#bersih2 lagi
resOwnData= ownershipData.filter(lambda x : x is not None)
resOwnData1=resOwnData.filter(lambda x : x[3] is not None)
resOwnData2=resOwnData1.filter(lambda x : x[4] is not None)
clean_data=resOwnData2.filter(lambda x : "San Francisco" in x[0])
ownData = clean_data.map(lambda x: (x[1],x[2],float(x[3]),float(x[4])))

ownerData = ownData.collect()

clust = [None]*len(ownerData)

fileObj = open("OwnershipData.csv", "wb")
csv_file = csv.writer(fileObj)  

for i in range(len(ownerData)):
    clust[i] = clusters.predict(data_local[i])
    temp = list(ownerData[i])
    temp.insert(4,clust[i])
    if (ownerData[i][1] != "n.a." and ownerData[i][1].isdigit()==True):
        csv_file.writerow(temp)

