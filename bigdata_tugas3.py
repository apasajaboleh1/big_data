from pyspark import SparkConf, SparkContext
import collections
import sys
conf = SparkConf().setMaster("local").setAppName("tugasbigdata")
sc = SparkContext(conf = conf)

#untuk mengetahui pemilik bisnis yang bisnisnya paling banyak di sanfrancisco
def parseLine(line):
    fields = line.split(',')
    #print len(fields)
    owner= fields[2]
    city = fields[5]
    count=1
    return (owner,city,count)
    
orig_stdout = sys.stdout
f = file('out.txt', 'w')
sys.stdout = f

lines = sc.textFile("file:///SparkCourse/data_center.csv")
parsedLines = lines.map(parseLine)
#bersih bersih
reserveddata= parsedLines.filter(lambda x : x is not None)
temp_data = reserveddata.filter(lambda x : "San Francisco" in x[1])
real_data= temp_data.map(lambda x : (x[0],x[2]))
max_person = real_data.reduceByKey(lambda x,y : x+y)
flipped_data= max_person.map(lambda (x,y): (y,x))
sorted_data= flipped_data.sortByKey()
data= sorted_data.collect()
jml_orang=0
nama_manusia=''
for res in data :
    print "nama orang : "+ str(res[1]) + " jumlah_bisnis: " + str(res[0])
    if jml_orang < res[0]:
        jml_orang=res[0]
        nama_manusia= res[1]
print "\n"
for res in data :
    if res[0]==jml_orang:
        print "orang yang bisnisnya paling besar : "+str(nama_manusia)+ "banyak usaha: "+ str(jml_orang)        


sys.stdout = orig_stdout
f.close()
