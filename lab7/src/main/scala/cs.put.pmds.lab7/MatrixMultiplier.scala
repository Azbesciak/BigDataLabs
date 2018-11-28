package cs.put.pmds.lab7

import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix

object MatrixMultiplier extends App {

	case class MatrixCell(x: Long, y: Long, value: Double)

	require(args.length == 3, "Expected 3 args: input path (matrix 1 and 2) and output")
	private val sc = Utils.getContext("Matrix Multiplication")

	def parseMatrix(path: String) = sc.textFile(path)
	 .map(splitLine)
	 .map(line => MatrixCell(line(0).toLong, line(1).toLong, line(2).toDouble))

	def coordMatrix(path: String) = new CoordinateMatrix(parseMatrix(path).map(v => MatrixEntry(v.x, v.y, v.value))).toBlockMatrix()

//	private val matrixM = parseMatrix(args(0)).map(t => (t.y, (t.x, t.value)))
//	private val matrixN = parseMatrix(args(1)).map(t => (t.x, (t.y, t.value)))
//	matrixM.join(matrixN)
//	 .map { case (_, ((i, v), (k, w))) => ((i, k), v * w) }
//	 .reduceByKey(_ + _)
//	 .sortByKey()
//	 .saveAsTextFile(args(2))

	coordMatrix(args(0))
	 .multiply(coordMatrix(args(1)))
	 .toCoordinateMatrix()
	 .entries
	 .sortBy(v => (v.i, v.j))
	 .saveAsTextFile(args(2))



	private def splitLine(line: String) = line.split("\\s+").map(_.trim)

}
