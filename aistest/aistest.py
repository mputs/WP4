
from pyspark import SparkConf
from pyspark import SparkContext


conf = SparkConf()
conf.setAppName('AIS-test')
conf.setMaster('yarn-client')
sc = SparkContext(conf = conf)

data = sc.textFile("hdfs://namenode.ib.sandbox.ichec.ie:8020/datasets/AIS/Locations/20151231*.csv.gz");
data1 = data.map(lambda line: line.split(","));
data2 = data1.filter(lambda x: x[0]!="mmsi");
print "Lines in dataset: ",  data2.count()
