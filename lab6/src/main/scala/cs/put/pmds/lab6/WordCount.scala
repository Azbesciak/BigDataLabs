package cs.put.pmds.lab6

import cs.put.pmds.lab6.Utils.getContext

object WordCount extends App {
	require(args.length == 2, "Expected 2 args: input path and output")
	private val sc = getContext("Word Count")
	private val textFile = sc textFile args(0)
	private val counts = textFile
	 .flatMap(line => line replaceAll("[^\\w\\s]", "") split "\\s+")
	 .filter(_.length > 0)
	 .map(_.toLowerCase)
	 .map(word => (word, 1))
	 .reduceByKey(_ + _)
	 .sortBy(-_._2)
	counts saveAsTextFile args(1)
}
