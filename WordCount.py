import sys
from operator import add
from pyspark import SparkContext
if __name__ == "__main__":
	if len(sys.argb) != 3:
		print >> sys.stderr, "Usage: wordcount <input> <output>"
		exit(-1)
	sc = SparkContext(appName = "PythonWordCount")
	lines = sc.textFile(sys.argvp[1], 1)
	counts = lines.flatMap(lambda x: x.split(' ')) \
				.map(lambda x:(x, 1)) \
				.reduceByKey(add)
	output = counts.collect()
	for (word, count) in output:
		print "%s: %i" % (word, count)
	counts.saveAsTextFile(sys.argv[2])
	sc.stop()