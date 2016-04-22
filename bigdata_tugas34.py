from pyspark import SparkConf, SparkContext
import collections,sys
from pyspark.mllib.clustering import KMeans, KMeansModel
from numpy import array
from math import sqrt
from mpl_toolkits.basemap import Basemap
import matplotlib.pyplot as plt

conf = SparkConf().setMaster("local").setAppName("tugasbigdata")
sc = SparkContext(conf = conf)
count=0
hasil_cluster=[]
hasil_data_smua=[]
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

def mapTut():

    m = Basemap(projection='mill',llcrnrlat=37,urcrnrlat=37.9,\
                llcrnrlon=-122.7,urcrnrlon=-122.1,resolution='c')
    m.drawcoastlines()
    m.drawcountries()
    m.drawstates()
    m.fillcontinents(color='#04BAE3',lake_color='#FFFFFF')
    m.drawmapboundary(fill_color='#FFFFFF')


    
    for a in hasil_data_smua:
        lat,lon = a[0],a[1]
        x,y = m(lon,lat)    
        m.plot(x,y, 'ro')
    for b in hasil_cluster :
        lat,lon = b[0],b[1]
        x,y = m(lon,lat)    
        m.plot(x,y, 'go')    
    
    plt.title("Geo Plotting")
    plt.show()

lines = sc.textFile("file:///SparkCourse/data_center.csv")
parsedLines = lines.map(parseLine)
#bersih bersih
reserveddata= parsedLines.filter(lambda x : x is not None)
reserveddata1= reserveddata.filter(lambda x : x[1] is not None)
reserveddata2=reserveddata1.filter(lambda x : x[2] is not None)
temp=reserveddata2.filter(lambda x : "San Francisco" in x[0])
tempdata=temp.map(lambda x: (x[1],x[2]))
data=tempdata.map(lambda x: (float(x[0]),float(x[1])))
point_data=data.collect()
for res_data in point_data :
    hasil_data_smua.append(res_data)

#cara 2
K=2
clusters = KMeans.train(data, K,maxIterations=200000,runs=20, initializationMode="random", epsilon=1e-8)

print clusters.clusterCenters
for temp12 in clusters.clusterCenters :
    hasil_cluster.append(temp12)
def error(point):
 center = clusters.centers[clusters.predict(point)]
 return sqrt(sum([x**2 for x in (point - center)]))

WSSSE = data.map(lambda point: error(point)).reduce(lambda x, y: x + y)
print("Within Set Sum of Squared Error = " + str(WSSSE))
mapTut()