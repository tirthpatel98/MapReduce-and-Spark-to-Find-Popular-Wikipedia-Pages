import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('example code').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

# add more functions as necessary

@functions.udf(returnType=types.StringType())
def path_to_hour(path):
    #print(path)
    arr=path.split("/")
    time= arr[-1][11:-7]
    return (time)
    
def main(inputs, output):
    # main logic starts here
    wiki_schema = types.StructType([
    types.StructField('language', types.StringType()),
    types.StructField('title', types.StringType()),
    types.StructField('views', types.IntegerType()),
    types.StructField('size', types.LongType()),
])
    #reading data
    wikiData = spark.read.csv(inputs, schema=wiki_schema, sep=" ").withColumn('hour', path_to_hour(functions.input_file_name()))
    #filtering data
    filteredWikiData = wikiData[(wikiData['language']=='en') & (wikiData['title']!='Main_Page') & (wikiData['title']!='Special:Page')].cache()
    #finding max views per hour.
    maxCount=filteredWikiData.groupBy('hour').agg(functions.max(filteredWikiData['views']).alias('max'))
    #joining data to obtain hour and title.
    joinData=filteredWikiData.join(maxCount,filteredWikiData.views==maxCount.max).select(filteredWikiData["hour"],filteredWikiData["title"],filteredWikiData["views"])
    #sorting data based on hour and storing it in json file.
    joinData.sort(functions.asc('hour')).write.json(output, mode='overwrite')
    #joinData.sort(functions.asc('hour')).show()
    
    
if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    #inputs="pagecounts-1"
    #output="wikiOutput"
    main(inputs, output)
