from pyspark import SparkContext,SparkConf
import sys

inputs=sys.argv[1]
output=sys.argv[2]

conf = SparkConf().setAppName('word count')
sc = SparkContext(conf=conf)


#print(text.take(10))

def createTuples(words):
    return words.split()

def convertToInt(i):
    i[3]=int(i[3])
    return i

def filterFun(rec):
    if (rec[1]=='en' and rec[2]!='Special:Page' and rec[2]!='Main_Page'):
        return rec

def keyValue(arg):
    key = arg[0]
    value = (arg[3],arg[2])
    yield(key,value)

def findMax(x,y):
     if(max(x[0],y[0])==x[0]):
        return x
     else:
        return y

def get_key(kv):
    return kv[0]

def tab_separated(kv):
    return "%s\t%s" % (kv[0], kv[1]) 
    
text = sc.textFile(inputs)
tuple1=text.map(createTuples)        #tuple1:each line converted to a tuple of five things
tuple2=tuple1.map(convertToInt)      #tuple2:count converted to int

filteredTuple = tuple2.filter(filterFun)   #filteredTuple:removed unwanted record
mapOutput=filteredTuple.flatMap(keyValue)	#mapOutput:converts into key and value pair	
reducedOutput=mapOutput.reduceByKey(findMax)	#output after performing reduction
outdata=reducedOutput.sortBy(get_key)		#sorted by key
outdata.map(tab_separated).saveAsTextFile(output)	#reformed output







    
