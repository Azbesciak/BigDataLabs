package cs.put.pmds.lab6

object Runner extends App {
	require(!args.isEmpty, "require at least nr of task")
	private val taskArgs: Array[String] = args.drop(1)
	args(0).toInt match {
		case 1 => WordCount main taskArgs
		case 2 => MatrixVectorMultiplication main taskArgs
		case 3 => ApproximatePI main taskArgs
		case x => throw new IllegalArgumentException(s"unknown option $x")
	}
}
