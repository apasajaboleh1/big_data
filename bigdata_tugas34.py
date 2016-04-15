from pyspark import SparkConf, SparkContext
import collections,sys
from pyspark.mllib.clustering import KMeans, KMeansModel
from numpy import array
from math import sqrt
conf = SparkConf().setMaster("local").setAppName("tugasbigdata")
sc = SparkContext(conf = conf)
count=0
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
"""  
orig_stdout = sys.stdout
f = file('out.txt', 'w')
sys.stdout = f
"""
lines = sc.textFile("file:///SparkCourse/data_center.csv")
parsedLines = lines.map(parseLine)
#bersih bersih
reserveddata= parsedLines.filter(lambda x : x is not None)
reserveddata1= reserveddata.filter(lambda x : x[1] is not None)
reserveddata2=reserveddata1.filter(lambda x : x[2] is not None)
temp=reserveddata2.filter(lambda x : "San Francisco" in x[0])
tempdata=temp.map(lambda x: (x[1],x[2]))
data=tempdata.map(lambda x: (float(x[0]),float(x[1])))
#babi =data.collect()
"""
for res in babi:
    print res
sys.stdout = orig_stdout
f.close()
""""""
K = 2

def dist(x1, y1, x2, y2):
  return (x1 - x2)**2 + (y1 - y2)**2

def cluster(x, y, centers):
  d = [dist(x, y, X, Y) for (X, Y) in centers]
  mD = d[0]
  mI = 0
  for i in range(len(d)):
    if (d[i] < mD):
      mD = d[i]
      mI = i
  return mI

def kmeansIter(data, centers):
  return data.map(lambda x: (cluster(x[0], x[1], centers), (1, x))).reduceByKey(lambda (n1, (x1, y1)), (n2, (x2, y2)): (n1+n2, (x1+x2, y1+y2))).map(lambda (k, (n, (x, y))): (x/n, y/n)).collect()

def kmeans(data, centers):
  delta = 1
  while (delta > 1e-8):
    centers1 = kmeansIter(data, centers)
    #print centers1
    d = [dist(x, y, X, Y) for ((x, y), (X, Y)) in zip(centers, centers1)]
    delta = max(d)
    centers = centers1
  return centers


run = 20
for i in range(run):
    centers = data.takeSample(withReplacement=False, num=K, seed=None)
    arr = kmeans(data, centers)
    if i == run-1:        
        print "centroid posisi awal " 
        print centers    
        print "centroid posisi akhir "
        print arr

"""
#cara 2
K=2
clusters = KMeans.train(data, K,maxIterations=200000,runs=20, initializationMode="random", epsilon=1e-8)

print clusters.clusterCenters

def error(point):
 center = clusters.centers[clusters.predict(point)]
 return sqrt(sum([x**2 for x in (point - center)]))

WSSSE = data.map(lambda point: error(point)).reduce(lambda x, y: x + y)
print("Within Set Sum of Squared Error = " + str(WSSSE))
