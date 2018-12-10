package cs.put.pmds.lab7


object Runner extends App {
	require(args.length > 0, "pass no. of task")
	val taskArgs = args.drop(1)
	args(0).toInt match {
		case 1 => Statistic main taskArgs
		case 2 => MatrixMultiplier main taskArgs
		case 3 => SongsQuerier main taskArgs
		case x => throw new IllegalStateException(s"unknown argument $x")
	}
}
