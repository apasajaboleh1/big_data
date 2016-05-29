from pyspark import SparkConf, SparkContext
import collections
import sys,logging
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.feature import IDF


conf = SparkConf().setMaster("local").setAppName("final project")
sc = SparkContext(conf = conf)

def parseLine(line):
    data_field=line.split("\t")
    if data_field[2]!='country':
        nama_univ=data_field[1].encode('utf-8')
        negara_univ=data_field[2]
        ranking_that_year=[]
        ranking_that_year.append(data_field[0])
        ranking_that_year.append(data_field[13])
        return (nama_univ,negara_univ,ranking_that_year)


orig_stdout = sys.stdout
f = file('test_dat.txt', 'w')
sys.stdout = f
 #log

#input data
input_data=''
if len(sys.argv) >2 :
    count=0
    for x in sys.argv[1:]:
        if count!=len(sys.argv)-2:
            input_data=input_data+x+" "
            count+=1
            
        else :
            input_data=input_data+x
"""input_data=[]
if len(sys.argv) >2 :
    count=0
    for x in sys.argv[1:]:
        input_data.append(x.lower())
        """
#print input_data
lines = sc.textFile("file:///SparkCourse/cwurData.txt")
parsedLines = lines.map(parseLine)
prepared_data=parsedLines.filter(lambda x:x is not None)
data=prepared_data.map(lambda x : (x[0],x[1],x[2]))
name_univ=prepared_data.map(lambda x: x[0].lower())
#for ji in name_univ.collect() :
#    print ji

hashingTF = HashingTF(100000)  #100K hash buckets just to save some memory
tf = hashingTF.transform(name_univ)

idf = IDF(minDocFreq=1).fit(tf)
tfidf =idf.transform(tf)
#for ji in tfidf.collect():
#    print ji
keyword_cari=hashingTF.transform(input_data.lower())
#print input_data
#print hashingTF.transform(input_data.lower())
keyhashvalue=int(keyword_cari.indices[0])
keyrelevance=tfidf.map(lambda x :x[keyhashvalue])

zippedResults = keyrelevance.zip(data).filter(lambda x : x[0]>0.0)
badak=zippedResults.collect()
print ("Data yang mungkin berkaitan dengan %s")%input_data
if len(badak) == 0 :
    print "no data"
else:
    for a in badak :
        print "universitas : ",
        print a[1][0],
        print " negara : ",
        print a[1][1],
        print " peringkat : ",
        print a[1][2][0],
        print " tahun : ",
        print a[1][2][1]
sys.stdout = orig_stdout
f.close()
