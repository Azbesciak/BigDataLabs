package cs.put.pmds.lab6

import cs.put.pmds.lab6.Utils.getContext
import scala.math.random

object ApproximatePI extends App {
	private val sc = getContext("Approximate PI")
	val slices = if (args.length < 0) args(0).toInt else 2
	val n = 1000000L * slices
	val xs = 1L until n
	val rdd = sc.parallelize(xs, slices).setName("'Initial rdd'")
	val sample = rdd.map { _ =>
		val x = random * 2 - 1
		val y = random * 2 - 1
		(x, y)
	}.setName("'Random points sample'")

	val inside = sample.filter { case (x, y) => x * x + y * y < 1 }.setName("'Random points inside circle'")

	val count = inside.count()

	println(f"Pi is roughly ${4.0 * count / n}%1.10f")
}
