from pyspark import SparkConf, SparkContext
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.feature import IDF
import os,sys

# Function for printing each element in RDD
def println(x):
    print x


# Boilerplate Spark stuff:
conf = SparkConf().setMaster("local").setAppName("SparkTFIDF")
sc = SparkContext(conf = conf)

# Load documents (one per line).
rawData = sc.textFile("tugas4.txt")
fields = rawData.map(lambda x: x.split("\t"))
documents = fields.map(lambda x: x[2].lower().split(" "))

documentNames = fields.map(lambda x: x[0])

hashingTF = HashingTF(100000)  #100K hash buckets just to save some memory
tf = hashingTF.transform(documents)

idf = IDF(minDocFreq=1).fit(tf) # ini minDocFreq untuk menentukan berapa jumlah document yang harus ada kata tersebut
tfidf = idf.transform(tf)

input= sys.argv[1] # ambil dari argument pada saat compile
#print hashingTF.transform([input.lower()])
#print hashingTF.transform([input.lower()]).indices[0]

keywordTF = hashingTF.transform([input.lower()])
keywordHashValue = int(keywordTF.indices[0])

keywordRelevance = tfidf.map(lambda x: x[keywordHashValue])

zippedResults = keywordRelevance.zip(documentNames).filter(lambda x : x[0]>0.0)
badak=zippedResults.collect()
print "Best id document for keywords is:"
if len(badak) == 0 :
    print "no data"
else:
    for a in badak :
        print a[1]
#print len(badak)
