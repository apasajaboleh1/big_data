from pyspark import SparkConf, SparkContext
import collections
import sys
conf = SparkConf().setMaster("local").setAppName("tugasbigdata")
sc = SparkContext(conf = conf)
#untuk mengetahui pemilik bisnis yang bisnisnya paling banyak di sanfrancisco
def parseLine(line) :
    fields = line.split(',')
    #print len(fields)
    class_code= fields[12]
    city = fields[5]
    count=1
    #print class_code,city,count
    return (class_code,city,count)

def data(line) :
    fields= line.split('.')
    #print "a"
    code=fields[0]
    name=fields[1]
    return (code,name)
orig_stdout = sys.stdout
f = file('out.txt', 'w')
sys.stdout = f

lines = sc.textFile("file:///SparkCourse/data_center.csv")
parsedLines = lines.map(parseLine)
lines = sc.textFile("file:///SparkCourse/data_pbc.txt")
com_lines = lines.map(data)
#bersih bersih
temp=parsedLines.filter(lambda x:x is not None)
temp1=temp.filter(lambda x:(x[0] >='1' and x[0]<='16') or (x[0]>='2' and x[0]<='9'))
temp2=temp1.filter(lambda x : "San Francisco" in x[1])
real_data= temp2.map(lambda x : (x[0],x[2]))
max_person = real_data.reduceByKey(lambda x,y : x+y)
flipped_data= max_person.map(lambda (x,y): (y,x))
sorted_data= flipped_data.sortByKey()
back_origin= sorted_data.map(lambda (x,y):(y,x))
data= back_origin.join(com_lines).collect()
for res in data :
    #print res
    print "jenis_bisnis : "+ str(res[1][1]) + " jumlah_stan: " + str(res[1][0])
sys.stdout = orig_stdout
f.close()