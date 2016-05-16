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

filePath =  "/usr/local/spark/big_data/modif/lalaa.txt"

def removeTheLine(lineToRemove):

    f = open(filePath,"r")
    lines = f.readlines()
    f.close()

    f = open(filePath,"w")
    linesInFile = 0
    for line in lines:
        linesInFile = linesInFile + 1
        if linesInFile != lineToRemove:
            f.write(line)
    f.close()

input= sys.argv[1]

# Load documents (one per line).

arrayOfId= []

for x in range (0,4):
    rawData = 1
    rawData = sc.textFile(filePath)
    fields = rawData.map(lambda x: x.split("\t"))
    documents = fields.map(lambda x: x[2].lower().split(" "))
    documentNames = fields.map(lambda x: x[0])


    hashingTF = HashingTF(100000)  #100K hash buckets just to save some memory
    tf = hashingTF.transform(documents)

    idf = IDF(minDocFreq=1).fit(tf) # ini minDocFreq untuk menentukan berapa jumlah document yang harus ada kata tersebut
    tfidf = idf.transform(tf)

    keywordTF = hashingTF.transform([input.lower()])
    keywordHashValue = int(keywordTF.indices[0])

    keywordRelevance = tfidf.map(lambda x: x[keywordHashValue])

    zippedResults = keywordRelevance.zip(documentNames)

    lala = zippedResults.max()
    print lala[1]
    removeTheLine(lala[1])

# print "Best id document for keywords is:"