from pyspark import SparkConf, SparkContext
import collections
import sys
conf = SparkConf().setMaster("local").setAppName("tugasbigdata")
sc = SparkContext(conf = conf)

#untuk mengetahui pemilik bisnis yang bisnisnya paling banyak di sanfrancisco
def parseLine(line):
    fields = line.split(',')
    #print len(fields)
    street= fields[4]
    city = fields[5]
    count=1
    return (street,city,count)
    
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
jml_stan=0
nama_jalan=''
for res in data :
    print "nama jalan : "+ str(res[1]) + "jumlah_stan: " + str(res[0])
    if jml_stan < res[0]:
        jml_stan=res[0]
        nama_jalan= res[1]
print "\n" + str(jml_stan)
for res in data :
    if res[0]==jml_stan :
        print "jalan yang menjadi pusat perdangan: "+str(res[1]) 
sys.stdout = orig_stdout
f.close()
