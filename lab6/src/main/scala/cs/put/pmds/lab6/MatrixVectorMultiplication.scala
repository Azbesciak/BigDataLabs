package cs.put.pmds.lab6

import cs.put.pmds.lab6.Utils.getContext


object MatrixVectorMultiplication extends App {
	require(args.length == 3, "Expected 3 args: input path (matrix and vector) and output")
	private val sc = getContext("Matrix Multiplication")
	private val matrix = sc.textFile(args(0))
	 .map(splitLine)
	 .map(line => (line(0).toInt, line(1).toInt, line(2).toDouble))

	private val vector = sc.textFile(args(1))
	 .map(splitLine)
	 .map(l => (l(0).toInt, l(1).toDouble))
	 .sortBy(_._1)
	 .map(_._2)
	 .collect

	private val broadcast = sc broadcast vector

	private val result = matrix.map { case (i, j, a) => (i, a * broadcast.value(j - 1)) }
	 .reduceByKey(_ + _)
	 .sortBy(_._1)

	result saveAsTextFile args(2)

	private def splitLine(line: String) = line.split(",").map(_.trim)
}
